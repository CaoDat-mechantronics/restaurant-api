// ======================================================
// ROBOT 1 - ESP32
//
// Backend -> HiveMQ:
//      topic1/command
//
// ESP32 -> HiveMQ:
//      topic1/status
//
// Command JSON ví dụ:
//
// {
//   "command_id": "A12BC345",
//   "robot": 1,
//   "item_id": 25,
//   "table": 3,
//   "food_name": "Pizza Hải Sản",
//   "action": "deliver"
// }
//
// Sau khi nhận:
//      t = table * 10 giây
//
// Bàn 3:
//      3 * 10 = 30 giây
//
// Sau 30 giây:
//      LED HIGH
//
// Và gửi:
//
// {
//   "command_id": "A12BC345",
//   "robot": 1,
//   "item_id": 25,
//   "table": 3,
//   "food_name": "Pizza Hải Sản",
//   "status": "delivered"
// }
// ======================================================


#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>


// ======================================================
// CẤU HÌNH ROBOT
// ======================================================

#define ROBOT_ID 1


// ======================================================
// LED
//
// Phần lớn ESP32 DevKit thường có LED ở GPIO 2.
//
// Nếu board của bạn dùng chân khác,
// sửa LED_PIN.
// ======================================================

#define LED_PIN 2


// ======================================================
// WIFI
// ======================================================

const char* WIFI_SSID =
    "Nhi Dat T4";


const char* WIFI_PASSWORD =
    "Alc476qu14_99";


// ======================================================
// HIVEMQ CLOUD
//
// Lấy Host trong HiveMQ Cloud.
//
// Ví dụ:
//
// abc123.s1.eu.hivemq.cloud
//
// KHÔNG thêm:
// mqtt://
// https://
// ======================================================

const char* MQTT_HOST =
    "20d0e023b23d4286a0527539aafdfe1e.s1.eu.hivemq.cloud";


const int MQTT_PORT =
    8883;


const char* MQTT_USERNAME =
    "dat.cao";


const char* MQTT_PASSWORD =
    "Kkta373376";


// ======================================================
// TOPIC ROBOT 1
// ======================================================

const char* COMMAND_TOPIC =
    "topic1/command";


const char* STATUS_TOPIC =
    "topic1/status";


// ======================================================
// THỜI GIAN RECONNECT
// ======================================================

const unsigned long WIFI_RECONNECT_INTERVAL =
    5000;


const unsigned long MQTT_RECONNECT_INTERVAL =
    5000;


const unsigned long STATUS_RETRY_INTERVAL =
    1000;


// ======================================================
// MQTT
// ======================================================

WiFiClientSecure secureClient;


PubSubClient mqttClient(
    secureClient
);


// ======================================================
// TRẠNG THÁI ROBOT
// ======================================================

// Robot đang có nhiệm vụ hay không
bool taskActive =
    false;


// Robot đã đến bàn chưa
bool arrivalReached =
    false;


// Thời điểm bắt đầu giao
unsigned long deliveryStartTime =
    0;


// Thời gian robot phải đi
unsigned long deliveryDuration =
    0;


// Thời điểm thử gửi status gần nhất
unsigned long lastStatusPublishAttempt =
    0;


// ======================================================
// THÔNG TIN NHIỆM VỤ HIỆN TẠI
// ======================================================

String currentCommandId =
    "";


int currentItemId =
    0;


int currentTable =
    0;


String currentFoodName =
    "";


// ======================================================
// COMMAND GẦN NHẤT HOÀN THÀNH
//
// Dùng để tránh xử lý trùng command.
// ======================================================

String lastCompletedCommandId =
    "";


// ======================================================
// TIMER RECONNECT
// ======================================================

unsigned long lastWiFiReconnectAttempt =
    0;


unsigned long lastMQTTReconnectAttempt =
    0;


// ======================================================
// PROTOTYPE
// ======================================================

void connectWiFi();

bool connectMQTT();

void mqttCallback(
    char* topic,
    byte* payload,
    unsigned int length
);

bool publishDeliveredStatus();

void publishBusyStatus(
    const String& commandId,
    int itemId,
    int tableNumber,
    const String& foodName
);

void processDelivery();

void maintainConnections();


// ======================================================
// SETUP
// ======================================================

void setup()
{
    // ==================================================
    // SERIAL
    // ==================================================

    Serial.begin(
        115200
    );


    delay(
        500
    );


    Serial.println();
    Serial.println(
        "========================================"
    );

    Serial.println(
        "       RESTAURANT ROBOT 1 - ESP32"
    );

    Serial.println(
        "========================================"
    );


    // ==================================================
    // LED
    // ==================================================

    pinMode(
        LED_PIN,
        OUTPUT
    );


    digitalWrite(
        LED_PIN,
        LOW
    );


    // ==================================================
    // WIFI MODE
    // ==================================================

    WiFi.mode(
        WIFI_STA
    );


    // ==================================================
    // TLS
    //
    // CHỈ DÙNG setInsecure() KHI TEST.
    //
    // Nó vẫn mã hóa TLS nhưng bỏ qua việc xác minh
    // certificate của server.
    //
    // Khi triển khai thật nên dùng:
    //
    // secureClient.setCACert(ROOT_CA);
    // ==================================================

    secureClient.setInsecure();


    // ==================================================
    // MQTT
    // ==================================================

    mqttClient.setServer(
        MQTT_HOST,
        MQTT_PORT
    );


    mqttClient.setCallback(
        mqttCallback
    );


    // Payload có tên món nên tăng buffer.
    mqttClient.setBufferSize(
        768
    );


    mqttClient.setKeepAlive(
        30
    );


    // ==================================================
    // CONNECT WIFI
    // ==================================================

    connectWiFi();


    // ==================================================
    // CONNECT MQTT
    // ==================================================

    connectMQTT();


    Serial.println();

    Serial.println(
        "Robot 1 san sang."
    );

    Serial.print(
        "Subscribe topic: "
    );

    Serial.println(
        COMMAND_TOPIC
    );


    Serial.println(
        "========================================"
    );
}


// ======================================================
// LOOP
// ======================================================

void loop()
{
    // ==================================================
    // GIỮ WIFI + MQTT
    // ==================================================

    maintainConnections();


    // ==================================================
    // MQTT LOOP
    //
    // Phải chạy thường xuyên.
    // ==================================================

    if (
        mqttClient.connected()
    )
    {
        mqttClient.loop();
    }


    // ==================================================
    // XỬ LÝ ROBOT ĐANG DI CHUYỂN
    // ==================================================

    processDelivery();


    // Delay rất nhỏ để CPU không chạy 100%
    delay(
        2
    );
}


// ======================================================
// CONNECT WIFI
// ======================================================

void connectWiFi()
{
    if (
        WiFi.status()
        ==
        WL_CONNECTED
    )
    {
        return;
    }


    Serial.println();

    Serial.print(
        "[WIFI] Dang ket noi: "
    );

    Serial.println(
        WIFI_SSID
    );


    WiFi.begin(
        WIFI_SSID,
        WIFI_PASSWORD
    );


    unsigned long startTime =
        millis();


    // Cho lần khởi động tối đa 15 giây.
    while (
        WiFi.status()
            !=
            WL_CONNECTED

        &&

        millis()
            -
            startTime
            <
            15000
    )
    {
        delay(
            500
        );

        Serial.print(
            "."
        );
    }


    Serial.println();


    if (
        WiFi.status()
        ==
        WL_CONNECTED
    )
    {
        Serial.println(
            "[WIFI] Ket noi thanh cong!"
        );


        Serial.print(
            "[WIFI] IP: "
        );


        Serial.println(
            WiFi.localIP()
        );


        Serial.print(
            "[WIFI] RSSI: "
        );


        Serial.print(
            WiFi.RSSI()
        );


        Serial.println(
            " dBm"
        );
    }
    else
    {
        Serial.println(
            "[WIFI] Chua ket noi duoc."
        );
    }
}


// ======================================================
// CONNECT MQTT
// ======================================================

bool connectMQTT()
{
    if (
        WiFi.status()
        !=
        WL_CONNECTED
    )
    {
        return false;
    }


    if (
        mqttClient.connected()
    )
    {
        return true;
    }


    Serial.println();

    Serial.print(
        "[MQTT] Connecting to "
    );

    Serial.print(
        MQTT_HOST
    );

    Serial.print(
        ":"
    );

    Serial.println(
        MQTT_PORT
    );


    // ==================================================
    // TẠO CLIENT ID RIÊNG CHO ESP32
    // ==================================================

    uint64_t chipId =
        ESP.getEfuseMac();


    String clientId =
        "restaurant-robot1-";


    clientId +=
        String(
            (uint32_t)
            (
                chipId
                &
                0xFFFFFFFF
            ),
            HEX
        );


    // ==================================================
    // MQTT LOGIN
    // ==================================================

    bool connected =
        mqttClient.connect(
            clientId.c_str(),
            MQTT_USERNAME,
            MQTT_PASSWORD
        );


    if (
        !connected
    )
    {
        Serial.print(
            "[MQTT] Ket noi that bai. state = "
        );


        Serial.println(
            mqttClient.state()
        );


        return false;
    }


    Serial.println(
        "[MQTT] Ket noi thanh cong!"
    );


    // ==================================================
    // SUBSCRIBE COMMAND
    // ==================================================

    bool subscribed =
        mqttClient.subscribe(
            COMMAND_TOPIC,
            1
        );


    if (
        subscribed
    )
    {
        Serial.print(
            "[MQTT] Subscribe OK: "
        );


        Serial.println(
            COMMAND_TOPIC
        );
    }
    else
    {
        Serial.println(
            "[MQTT] Subscribe that bai!"
        );
    }


    return subscribed;
}


// ======================================================
// DUY TRÌ WIFI + MQTT
// ======================================================

void maintainConnections()
{
    unsigned long now =
        millis();


    // ==================================================
    // WIFI
    // ==================================================

    if (
        WiFi.status()
        !=
        WL_CONNECTED
    )
    {
        if (
            now
            -
            lastWiFiReconnectAttempt
            >=
            WIFI_RECONNECT_INTERVAL
        )
        {
            lastWiFiReconnectAttempt =
                now;


            Serial.println(
                "[WIFI] Mat ket noi. Thu ket noi lai..."
            );


            WiFi.disconnect();


            WiFi.begin(
                WIFI_SSID,
                WIFI_PASSWORD
            );
        }


        return;
    }


    // ==================================================
    // MQTT
    // ==================================================

    if (
        !mqttClient.connected()
    )
    {
        if (
            now
            -
            lastMQTTReconnectAttempt
            >=
            MQTT_RECONNECT_INTERVAL
        )
        {
            lastMQTTReconnectAttempt =
                now;


            Serial.println(
                "[MQTT] Mat ket noi. Thu ket noi lai..."
            );


            connectMQTT();
        }
    }
}


// ======================================================
// CALLBACK MQTT
//
// Backend gửi:
//
// topic1/command
//
// {
//     "command_id": "ABC123",
//     "robot": 1,
//     "item_id": 25,
//     "table": 3,
//     "food_name": "Pizza Hai San",
//     "action": "deliver"
// }
// ======================================================

void mqttCallback(
    char* topic,
    byte* payload,
    unsigned int length
)
{
    // ==================================================
    // CHUYỂN PAYLOAD -> STRING
    // ==================================================

    String message =
        "";


    message.reserve(
        length
        +
        1
    );


    for (
        unsigned int i = 0;
        i < length;
        i++
    )
    {
        message +=
            (char)
            payload[i];
    }


    Serial.println();
    Serial.println(
        "========================================"
    );

    Serial.println(
        "[MQTT] NHAN COMMAND"
    );


    Serial.print(
        "Topic: "
    );

    Serial.println(
        topic
    );


    Serial.print(
        "Payload: "
    );

    Serial.println(
        message
    );


    // ==================================================
    // PARSE JSON
    // ==================================================

    JsonDocument doc;


    DeserializationError error =
        deserializeJson(
            doc,
            message
        );


    if (
        error
    )
    {
        Serial.print(
            "[JSON] Loi parse: "
        );


        Serial.println(
            error.c_str()
        );


        return;
    }


    // ==================================================
    // LẤY FIELD
    // ==================================================

    const char* action =
        doc["action"]
        |
        "";


    int robotNumber =
        doc["robot"]
        |
        0;


    int incomingItemId =
        doc["item_id"]
        |
        0;


    int incomingTable =
        doc["table"]
        |
        0;


    const char* commandIdCStr =
        doc["command_id"]
        |
        "";


    const char* foodNameCStr =
        doc["food_name"]
        |
        "";


    String incomingCommandId =
        String(
            commandIdCStr
        );


    String incomingFoodName =
        String(
            foodNameCStr
        );


    // ==================================================
    // KIỂM TRA ACTION
    // ==================================================

    if (
        String(
            action
        )
        !=
        "deliver"
    )
    {
        Serial.println(
            "[COMMAND] Action khong hop le."
        );


        return;
    }


    // ==================================================
    // KIỂM TRA ĐÚNG ROBOT 1
    // ==================================================

    if (
        robotNumber
        !=
        ROBOT_ID
    )
    {
        Serial.print(
            "[COMMAND] Command danh cho Robot "
        );


        Serial.print(
            robotNumber
        );


        Serial.println(
            ", khong phai Robot 1."
        );


        return;
    }


    // ==================================================
    // VALIDATE
    // ==================================================

    if (
        incomingCommandId.length()
            ==
            0

        ||

        incomingItemId
            <=
            0

        ||

        incomingTable
            <=
            0
    )
    {
        Serial.println(
            "[COMMAND] Du lieu command khong hop le."
        );


        return;
    }


    // ==================================================
    // COMMAND ĐANG XỬ LÝ BỊ GỬI LẶP
    // ==================================================

    if (
        taskActive

        &&

        incomingCommandId
            ==
            currentCommandId
    )
    {
        Serial.println(
            "[COMMAND] Command dang duoc xu ly -> bo qua duplicate."
        );


        return;
    }


    // ==================================================
    // ROBOT ĐANG BẬN
    // ==================================================

    if (
        taskActive
    )
    {
        Serial.println(
            "[ROBOT] Robot dang ban!"
        );


        Serial.print(
            "[ROBOT] Dang giao item "
        );


        Serial.print(
            currentItemId
        );


        Serial.print(
            " toi Ban "
        );


        Serial.println(
            currentTable
        );


        // Gửi busy về broker.
        // Backend hiện tại có thể chỉ log message này.
        publishBusyStatus(
            incomingCommandId,
            incomingItemId,
            incomingTable,
            incomingFoodName
        );


        return;
    }


    // ==================================================
    // COMMAND ĐÃ HOÀN THÀNH TRƯỚC ĐÓ
    // ==================================================

    if (
        incomingCommandId
        ==
        lastCompletedCommandId
    )
    {
        Serial.println(
            "[COMMAND] Command nay da hoan thanh -> bo qua."
        );


        return;
    }


    // ==================================================
    // NHẬN NHIỆM VỤ MỚI
    // ==================================================

    currentCommandId =
        incomingCommandId;


    currentItemId =
        incomingItemId;


    currentTable =
        incomingTable;


    currentFoodName =
        incomingFoodName;


    taskActive =
        true;


    arrivalReached =
        false;


    // ==================================================
    // t = table * 10 giây
    //
    // milliseconds:
    //
    // table * 10 * 1000
    // ==================================================

    deliveryDuration =
        (unsigned long)
        currentTable
        *
        10UL
        *
        1000UL;


    deliveryStartTime =
        millis();


    lastStatusPublishAttempt =
        0;


    // LED tắt khi bắt đầu hành trình
    digitalWrite(
        LED_PIN,
        LOW
    );


    // ==================================================
    // SERIAL
    // ==================================================

    Serial.println(
        "----------------------------------------"
    );


    Serial.println(
        "[ROBOT] BAT DAU GIAO"
    );


    Serial.print(
        "Robot      : "
    );

    Serial.println(
        ROBOT_ID
    );


    Serial.print(
        "Command ID : "
    );

    Serial.println(
        currentCommandId
    );


    Serial.print(
        "Item ID    : "
    );

    Serial.println(
        currentItemId
    );


    Serial.print(
        "Mon an     : "
    );

    Serial.println(
        currentFoodName
    );


    Serial.print(
        "Ban        : "
    );

    Serial.println(
        currentTable
    );


    Serial.print(
        "Thoi gian  : "
    );

    Serial.print(
        deliveryDuration
        /
        1000UL
    );


    Serial.println(
        " giay"
    );


    Serial.println(
        "----------------------------------------"
    );
}


// ======================================================
// XỬ LÝ THỜI GIAN GIAO
//
// KHÔNG DÙNG delay(table * 10000)
//
// để mqttClient.loop() vẫn chạy.
// ======================================================

void processDelivery()
{
    if (
        !taskActive
    )
    {
        return;
    }


    unsigned long now =
        millis();


    // ==================================================
    // CHƯA ĐẾN BÀN
    // ==================================================

    if (
        !arrivalReached
    )
    {
        unsigned long elapsed =
            now
            -
            deliveryStartTime;


        if (
            elapsed
            >=
            deliveryDuration
        )
        {
            arrivalReached =
                true;


            // ==========================================
            // LED HIGH
            // ==========================================

            digitalWrite(
                LED_PIN,
                HIGH
            );


            Serial.println();
            Serial.println(
                "========================================"
            );


            Serial.println(
                "[ROBOT] DA DEN BAN!"
            );


            Serial.print(
                "Ban     : "
            );

            Serial.println(
                currentTable
            );


            Serial.print(
                "Item ID : "
            );

            Serial.println(
                currentItemId
            );


            Serial.print(
                "Mon     : "
            );

            Serial.println(
                currentFoodName
            );


            Serial.println(
                "LED     : HIGH"
            );


            Serial.println(
                "Dang gui trang thai delivered..."
            );


            Serial.println(
                "========================================"
            );
        }
    }


    // ==================================================
    // ĐÃ ĐẾN BÀN
    //
    // Thử publish delivered.
    // Nếu mất MQTT thì chờ reconnect rồi gửi lại.
    // ==================================================

    if (
        arrivalReached
    )
    {
        if (
            now
            -
            lastStatusPublishAttempt
            >=
            STATUS_RETRY_INTERVAL
        )
        {
            lastStatusPublishAttempt =
                now;


            if (
                mqttClient.connected()
            )
            {
                bool success =
                    publishDeliveredStatus();


                if (
                    success
                )
                {
                    Serial.println(
                        "[ROBOT] Gui delivered thanh cong!"
                    );


                    Serial.print(
                        "[ROBOT] Topic: "
                    );


                    Serial.println(
                        STATUS_TOPIC
                    );


                    // ==================================
                    // HOÀN THÀNH
                    // ==================================

                    lastCompletedCommandId =
                        currentCommandId;


                    taskActive =
                        false;


                    arrivalReached =
                        false;


                    Serial.println(
                        "[ROBOT] Nhiem vu hoan thanh."
                    );


                    Serial.println(
                        "========================================"
                    );
                }
                else
                {
                    Serial.println(
                        "[ROBOT] Publish delivered that bai. Se thu lai..."
                    );
                }
            }
            else
            {
                Serial.println(
                    "[ROBOT] MQTT chua ket noi. Cho reconnect de gui delivered..."
                );
            }
        }
    }
}


// ======================================================
// PUBLISH DELIVERED
//
// topic1/status
// ======================================================

bool publishDeliveredStatus()
{
    if (
        !mqttClient.connected()
    )
    {
        return false;
    }


    JsonDocument doc;


    doc["command_id"] =
        currentCommandId;


    doc["robot"] =
        ROBOT_ID;


    doc["item_id"] =
        currentItemId;


    doc["table"] =
        currentTable;


    doc["food_name"] =
        currentFoodName;


    doc["status"] =
        "delivered";


    String output;


    serializeJson(
        doc,
        output
    );


    Serial.print(
        "[MQTT] SEND: "
    );


    Serial.println(
        output
    );


    return mqttClient.publish(
        STATUS_TOPIC,
        output.c_str(),
        false
    );
}


// ======================================================
// PUBLISH BUSY
//
// Khi backend gửi command khác nhưng robot
// vẫn đang thực hiện command trước.
// ======================================================

void publishBusyStatus(
    const String& commandId,
    int itemId,
    int tableNumber,
    const String& foodName
)
{
    if (
        !mqttClient.connected()
    )
    {
        return;
    }


    JsonDocument doc;


    doc["command_id"] =
        commandId;


    doc["robot"] =
        ROBOT_ID;


    doc["item_id"] =
        itemId;


    doc["table"] =
        tableNumber;


    doc["food_name"] =
        foodName;


    doc["status"] =
        "busy";


    doc["current_item_id"] =
        currentItemId;


    doc["current_table"] =
        currentTable;


    String output;


    serializeJson(
        doc,
        output
    );


    mqttClient.publish(
        STATUS_TOPIC,
        output.c_str(),
        false
    );


    Serial.print(
        "[MQTT] BUSY: "
    );


    Serial.println(
        output
    );
}