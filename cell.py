import RPi.GPIO as GPIO
import spidev
import time
from datetime import datetime
from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import threading
import json
from utils.command_line_utils import CommandLineUtils

# Command line arguments for AWS IoT configuration
cmdData = CommandLineUtils.parse_sample_input_pubsub()

received_count = 0
received_all_event = threading.Event()

# GPIO 핀 설정
DT_PIN = 5
SCK_PIN = 6

# SPI 채널 설정
SPI_CHANNEL = 0
SPI_DEVICE = 0

# MCP3008 설정
spi = spidev.SpiDev()
spi.open(SPI_CHANNEL, SPI_DEVICE)
spi.max_speed_hz = 1350000

# MCP3008에서 아날로그 데이터를 읽는 함수
def read_adc(adcnum):
    if adcnum > 7 or adcnum < 0:
        return -1
    r = spi.xfer2([1, (8 + adcnum) << 4, 0])
    adcout = ((r[1] & 3) << 8) + r[2]
    return adcout

class HX711:
    def __init__(self, dout, pd_sck, gain=128):
        self.dout = dout
        self.pd_sck = pd_sck
        self.gain = gain
        self.offset = 0
        self.scale = 1

        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.dout, GPIO.IN)
        GPIO.setup(self.pd_sck, GPIO.OUT)

        self.set_gain(gain)
        time.sleep(1)

    def set_gain(self, gain):
        if gain == 128:
            self.gain = 1
        elif gain == 64:
            self.gain = 3
        elif gain == 32:
            self.gain = 2
        GPIO.output(self.pd_sck, False)
        self.read()

    def read(self):
        count = 0
        while GPIO.input(self.dout):
            pass
        for _ in range(24):
            GPIO.output(self.pd_sck, True)
            count = count << 1
            GPIO.output(self.pd_sck, False)
            if GPIO.input(self.dout):
                count += 1
        for _ in range(self.gain):
            GPIO.output(self.pd_sck, True)
            GPIO.output(self.pd_sck, False)
        count ^= 0x800000
        return count

    def read_average(self, times=3):
        sum = 0
        for _ in range(times):
            sum += self.read()
        average = sum / times
        return average

    def tare(self, times=15):
        self.offset = self.read_average(times)
        print(f"Offset: {self.offset}")

    def set_scale(self, scale):
        self.scale = scale

    def get_weight(self, times=3):
        value = self.read_average(times) - self.offset
        weight = value / self.scale
        return weight

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))
    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))
    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if received_count == cmdData.input_count:
        received_all_event.set()

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

def measure_and_publish_data(mqtt_connection, topic):
    hx = HX711(dout=DT_PIN, pd_sck=SCK_PIN)
    hx.tare()
    print("Tare done, place a known weight on the scale")
    input("Press Enter to continue...")

    known_weight = 116  # Known weight in grams
    raw_data = hx.read_average(10)
    print(f"Raw data for scale setting: {raw_data}")
    scale = raw_data / known_weight
    hx.set_scale(scale)
    print(f"Scale set: {scale}")

    count = 0
    try:
        while True:
            weight = hx.get_weight(1)
            water_level = read_adc(0)  # MCP3008의 CH0에서 수위 센서 값 읽기
            now = datetime.now().strftime("%Y-%m-%d %H:%M KST")
            message = {
                "weight": weight,
                "water_level": water_level,
                "time": now,
                "count": count
            }
            print(f"Publishing message {count}: {message}")
            mqtt_connection.publish(
                topic=topic,
                payload=json.dumps(message),
                qos=mqtt.QoS.AT_LEAST_ONCE)

            count += 1
            time.sleep(2)  # 2초마다 데이터 전송
    except (KeyboardInterrupt, SystemExit):
        print("Cleaning up...")
    finally:
        GPIO.cleanup()
        spi.close()

if __name__ == '__main__':
    # Create the proxy options if the data is present in cmdData
    proxy_options = None
    if cmdData.input_proxy_host is not None and cmdData.input_proxy_port != 0:
        proxy_options = http.HttpProxyOptions(
            host_name=cmdData.input_proxy_host,
            port=cmdData.input_proxy_port)

    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=cmdData.input_endpoint,
        port=cmdData.input_port,
        cert_filepath=cmdData.input_cert,
        pri_key_filepath=cmdData.input_key,
        ca_filepath=cmdData.input_ca,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=cmdData.input_clientId,
        clean_session=False,
        keep_alive_secs=30,
        http_proxy_options=proxy_options,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    if not cmdData.input_is_ci:
        print(f"Connecting to {cmdData.input_endpoint} with client ID '{cmdData.input_clientId}'...")
    else:
        print("Connecting to endpoint with client ID")
    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    message_topic = cmdData.input_topic

    # Subscribe to topic
    print("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    # Start measuring and publishing data
    measure_and_publish_data(mqtt_connection, message_topic)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
