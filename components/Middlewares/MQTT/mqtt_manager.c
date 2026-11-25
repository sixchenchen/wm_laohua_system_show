#include "mqtt_manager.h"
#include "mqtt_client.h"
#include "esp_log.h"
#include <string.h>
#include "mqtt_config.h"
#include "mqtt_topic.h"
#include "mqtt_message.h"
#include "device_context.h"
#include "wifi_manager.h"

static const char *TAG = "mqtt_manager";

/* ─────────────── 模块常量 ─────────────── */

#define MQTT_KEEPALIVE_SEC 15     /* MQTT keepalive 心跳间隔 (秒) */
#define MQTT_CONN_TIMEOUT_MS 5000 /* MQTT 连接超时 (ms) */
#define MQTT_PUBLISH_RETRY 3      /* publish 重试次数 */
#define MQTT_PUBLISH_DELAY_MS 20  /* publish 重试间隔 (ms) */

static esp_mqtt_client_handle_t mqtt_client = NULL;
static mqtt_rx_callback_t rx_callback = NULL;
static mqtt_status_callback_t status_callback = NULL;
static char *will_message = NULL;
static bool s_is_provisioned = false;
static mqtt_config_t s_active_config;

// MQTT 状态
static mqtt_state_t mqtt_state = MQTT_STATE_UNINIT;

//  状态转字符串
static const char *state_to_string(mqtt_state_t state)
{
    switch (state)
    {
    case MQTT_STATE_UNINIT:
        return "UNINIT";
    case MQTT_STATE_INIT:
        return "INIT";
    case MQTT_STATE_STARTING:
        return "STARTING";
    case MQTT_STATE_RUNNING:
        return "RUNNING";
    case MQTT_STATE_STOPPING:
        return "STOPPING";
    case MQTT_STATE_STOPPED:
        return "STOPPED";
    case MQTT_STATE_ERROR:
        return "ERROR";
    default:
        return "UNKNOWN";
    }
}

//  状态变更
static void mqtt_set_state(mqtt_state_t new_state)
{
    if (mqtt_state != new_state)
    {
        mqtt_state = new_state;
        ESP_LOGI(TAG, "状态变更: %s", state_to_string(new_state));
        if (status_callback)
        {
            status_callback(new_state);
        }
    }
}

// 默认配置（未注册）连接成功后，将实际连接参数写回 NVS
static void save_default_config_if_needed(void)
{
    if (s_is_provisioned)
    {
        return;
    }

    mqtt_config_t stored;
    if (mqtt_config_load_stored(&stored) == ESP_OK &&
        strcmp(stored.broker_uri, s_active_config.broker_uri) == 0 &&
        strcmp(stored.client_id, s_active_config.client_id) == 0 &&
        strcmp(stored.username, s_active_config.username) == 0)
    {
        return;
    }

    mqtt_config_t config = s_active_config;
    config.is_provisioned = false;
    if (mqtt_config_save(&config) == ESP_OK)
    {
        ESP_LOGI(TAG, "默认配置连接成功，已写入 NVS: broker=%s", config.broker_uri);
    }
    else
    {
        ESP_LOGW(TAG, "默认配置写入 NVS 失败");
    }
}

//  MQTT事件处理
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = event_data;
    switch (event_id)
    {
    case MQTT_EVENT_BEFORE_CONNECT:
        ESP_LOGI(TAG, "MQTT 正在连接: broker=%s, client_id=%s",
                 s_active_config.broker_uri, s_active_config.client_id);
        break;

    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "MQTT 连接成功: broker=%s, session_present=%d",
                 s_active_config.broker_uri, event->session_present);
        wifi_manager_set_mqtt_ready(true);

        if (s_is_provisioned)
        {
            // 正式运行阶段：订阅所有业务 topic
            mqtt_manager_subscribe(mqtt_topic_control(), 1);
            mqtt_manager_subscribe(mqtt_topic_ota(), 1);
            mqtt_manager_subscribe(mqtt_topic_ota_broadcast(), 1);
            mqtt_manager_subscribe(mqtt_topic_config(), 1);
        }
        // provision config response 始终订阅（正式阶段也可能收到 config 更新）
        mqtt_manager_subscribe(mqtt_topic_provision_config(), 1);

        // 使用默认配置连接成功后，把当前连接参数写回 NVS，避免下次启动继续尝试旧地址
        save_default_config_if_needed();

        // 订阅完成后再置为 RUNNING，确保状态回调里启动注册时订阅已就绪
        mqtt_set_state(MQTT_STATE_RUNNING);
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "MQTT 断开: broker=%s", s_active_config.broker_uri);
        // 状态由 WiFi 驱动改变
        break;

    case MQTT_EVENT_DATA:
    {
        char topic[128] = {0};
        if (event->topic_len < sizeof(topic))
        {
            memcpy(topic, event->topic, event->topic_len);
        }
        if (rx_callback)
        {
            rx_callback(topic, (const uint8_t *)event->data, event->data_len);
        }
        break;
    }

    case MQTT_EVENT_ERROR:
        if (event->error_handle != NULL)
        {
            ESP_LOGE(TAG, "MQTT 错误: broker=%s, type=%d, tls_last_err=0x%x, "
                          "tls_stack_err=0x%x, cert_flags=0x%x, sock_errno=%d, connack=%d",
                     s_active_config.broker_uri,
                     event->error_handle->error_type,
                     event->error_handle->esp_tls_last_esp_err,
                     event->error_handle->esp_tls_stack_err,
                     event->error_handle->esp_tls_cert_verify_flags,
                     event->error_handle->esp_transport_sock_errno,
                     event->error_handle->connect_return_code);
        }
        else
        {
            ESP_LOGE(TAG, "MQTT 错误: broker=%s（无错误详情）", s_active_config.broker_uri);
        }
        mqtt_set_state(MQTT_STATE_ERROR);
        break;

    default:
        break;
    }
}

// 比较两份 MQTT 配置是否一致
static bool mqtt_config_equal(const mqtt_config_t *a, const mqtt_config_t *b)
{
    return strcmp(a->broker_uri, b->broker_uri) == 0 &&
           strcmp(a->client_id, b->client_id) == 0 &&
           strcmp(a->username, b->username) == 0 &&
           strcmp(a->password, b->password) == 0 &&
           strcmp(a->will_topic, b->will_topic) == 0 &&
           a->is_provisioned == b->is_provisioned;
}

// 使用指定配置初始化
esp_err_t mqtt_manager_init_with_config(const mqtt_config_t *config)
{
    if (config == NULL || config->broker_uri[0] == '\0')
    {
        ESP_LOGE(TAG, "MQTT 配置无效");
        return ESP_ERR_INVALID_ARG;
    }

    if (mqtt_client != NULL)
    {
        return ESP_OK;
    }

    esp_log_level_set("mqtt_client", ESP_LOG_NONE);

    // 保存一份副本，确保字符串生命周期覆盖 MQTT 客户端运行期
    s_active_config = *config;
    s_is_provisioned = s_active_config.is_provisioned;

    ESP_LOGI(TAG, "MQTT 目标服务器: %s", s_active_config.broker_uri);
    ESP_LOGI(TAG, "MQTT 连接参数: client_id=%s, username=%s, password=%s, "
                  "keepalive=%ds, timeout=%dms, provisioned=%d",
             s_active_config.client_id,
             s_active_config.username[0] ? s_active_config.username : "(空)",
             s_active_config.password[0] ? "******" : "(空)",
             MQTT_KEEPALIVE_SEC, MQTT_CONN_TIMEOUT_MS, s_is_provisioned);

    mqtt_topic_init();
    will_message = mqtt_message_create_will();
    if (will_message == NULL)
    {
        return ESP_FAIL;
    }

    esp_mqtt_client_config_t mqtt_cfg =
        {
            .broker.address.uri = s_active_config.broker_uri,
            .credentials =
                {
                    .client_id = s_active_config.client_id,
                    .username = s_active_config.username,
                    .authentication.password = s_active_config.password,
                },
            .session =
                {
                    .last_will =
                        {
                            .topic = s_active_config.will_topic,
                            .msg = will_message,
                            .msg_len = strlen(will_message),
                            .qos = WILL_QOS,
                            .retain = WILL_RETAIN,
                        },
                    .keepalive = MQTT_KEEPALIVE_SEC,
                },
            .network = {
                .timeout_ms = MQTT_CONN_TIMEOUT_MS,
                .disable_auto_reconnect = false,
            }

    };

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    if (mqtt_client == NULL)
    {
        free(will_message);
        will_message = NULL;
        return ESP_FAIL;
    }

    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    mqtt_set_state(MQTT_STATE_INIT);
    return ESP_OK;
}

// 初始化：从 NVS 加载配置（NVS 缺失时使用默认值）
esp_err_t mqtt_manager_init(void)
{
    mqtt_config_t config;
    esp_err_t ret = mqtt_config_load(&config);
    if (ret != ESP_OK)
    {
        return ret;
    }
    return mqtt_manager_init_with_config(&config);
}

// 应用新配置：配置相同则复用客户端，不同则重建
esp_err_t mqtt_manager_reconfigure(const mqtt_config_t *config)
{
    if (config == NULL || config->broker_uri[0] == '\0')
    {
        return ESP_ERR_INVALID_ARG;
    }

    if (mqtt_client != NULL)
    {
        if (mqtt_config_equal(&s_active_config, config))
        {
            s_is_provisioned = config->is_provisioned;
            return ESP_OK;
        }

        ESP_LOGI(TAG, "MQTT 配置已变化，重建客户端");
        mqtt_manager_destroy();
    }

    return mqtt_manager_init_with_config(config);
}

// 启动
esp_err_t mqtt_manager_start(void)
{
    if (mqtt_client == NULL)
    {
        ESP_LOGE(TAG, "MQTT 未初始化");
        return ESP_FAIL;
    }

    if (mqtt_state == MQTT_STATE_RUNNING || mqtt_state == MQTT_STATE_STARTING)
    {
        ESP_LOGW(TAG, "MQTT 已启动或正在启动");
        return ESP_OK;
    }

    mqtt_set_state(MQTT_STATE_STARTING);
    esp_err_t ret = esp_mqtt_client_start(mqtt_client);

    if (ret == ESP_OK)
    {
        ESP_LOGI(TAG, "MQTT 启动成功，等待连接...");
    }
    else
    {
        ESP_LOGE(TAG, "MQTT 启动失败: %d", ret);
        mqtt_set_state(MQTT_STATE_ERROR);
    }
    return ret;
}

// 停止
esp_err_t mqtt_manager_stop(void)
{
    if (mqtt_client == NULL)
    {
        return ESP_OK;
    }

    if (mqtt_state == MQTT_STATE_STOPPED || mqtt_state == MQTT_STATE_UNINIT)
    {
        return ESP_OK;
    }

    mqtt_set_state(MQTT_STATE_STOPPING);
    esp_err_t ret = esp_mqtt_client_stop(mqtt_client);

    if (ret == ESP_OK)
    {
        mqtt_set_state(MQTT_STATE_STOPPED);
        ESP_LOGI(TAG, "MQTT 已停止");
    }
    else
    {
        ESP_LOGE(TAG, "MQTT 停止失败: %d", ret);
    }
    return ret;
}

// 销毁
esp_err_t mqtt_manager_destroy(void)
{
    if (mqtt_client)
    {
        if (mqtt_state != MQTT_STATE_STOPPED && mqtt_state != MQTT_STATE_UNINIT)
        {
            esp_mqtt_client_stop(mqtt_client);
        }
        esp_mqtt_client_destroy(mqtt_client);
        mqtt_client = NULL;
    }
    if (will_message)
    {
        free(will_message);
        will_message = NULL;
    }
    mqtt_set_state(MQTT_STATE_UNINIT);
    return ESP_OK;
}
// 状态查询
mqtt_state_t mqtt_manager_get_state(void)
{
    return mqtt_state;
}

const char *mqtt_manager_get_state_string(void)
{
    return state_to_string(mqtt_state);
}

bool mqtt_manager_is_running(void)
{
    return mqtt_state == MQTT_STATE_RUNNING;
}

// WiFi 状态驱动
void mqtt_manager_on_wifi_connected(void)
{
    ESP_LOGI(TAG, "WiFi 已连接，当前 MQTT 状态: %s", state_to_string(mqtt_state));
    if (mqtt_state == MQTT_STATE_INIT || mqtt_state == MQTT_STATE_STOPPED)
    {
        mqtt_manager_start();
    }
    else if (mqtt_state == MQTT_STATE_ERROR)
    {
        ESP_LOGW(TAG, "MQTT 错误状态，重新初始化");
        mqtt_manager_destroy();
        mqtt_manager_init();
        mqtt_manager_start();
    }
    else
    {
        ESP_LOGI(TAG, "MQTT 已运行，无需操作");
    }
}

void mqtt_manager_on_wifi_disconnected(void)
{
    if (mqtt_state == MQTT_STATE_RUNNING || mqtt_state == MQTT_STATE_STARTING)
    {
        ESP_LOGW(TAG, "WiFi 断开，停止 MQTT");
        mqtt_manager_stop();
    }
    else
    {
        ESP_LOGI(TAG, "MQTT 未运行，无需停止");
    }
}

// 发布消息
esp_err_t mqtt_manager_publish(const char *topic, const char *data, int len, int qos, bool retain)
{
    if (topic == NULL || data == NULL)
    {
        return ESP_ERR_INVALID_ARG;
    }
    if (mqtt_client == NULL)
    {
        ESP_LOGD(TAG, "MQTT 客户端为空");
        return ESP_FAIL;
    }
    if (mqtt_state != MQTT_STATE_RUNNING)
    {
        ESP_LOGD(TAG, "MQTT 未运行 (状态: %s)", state_to_string(mqtt_state));
        return ESP_FAIL;
    }

    // 带重试的 publish：瞬时资源紧张（如 OTA 启动抢 WiFi）时重试
    int msg_id = -1;
    for (int attempt = 0; attempt < MQTT_PUBLISH_RETRY; attempt++)
    {
        msg_id = esp_mqtt_client_publish(mqtt_client, topic, data, len, qos, retain);
        if (msg_id >= 0)
            break;
        if (attempt < MQTT_PUBLISH_RETRY - 1)
            vTaskDelay(pdMS_TO_TICKS(MQTT_PUBLISH_DELAY_MS));
    }

    if (msg_id < 0)
    {
        return ESP_FAIL;
    }
    ESP_LOGI(TAG, "发布 id=%d topic=%s", msg_id, topic);
    return ESP_OK;
}

esp_err_t mqtt_manager_subscribe(const char *topic, int qos)
{
    if (mqtt_client == NULL)
    {
        ESP_LOGE(TAG, "subscribe FAIL: client is NULL, topic=%s", topic);
        return ESP_FAIL;
    }
    int id = esp_mqtt_client_subscribe(mqtt_client, topic, qos);
    if (id >= 0)
    {
        ESP_LOGI(TAG, "subscribe OK: topic=%s qos=%d mid=%d", topic, qos, id);
        return ESP_OK;
    }
    else
    {
        ESP_LOGE(TAG, "subscribe FAIL: topic=%s qos=%d ret=%d (see ESP_ERR_MQTT_*)", topic, qos, id);
        return ESP_FAIL;
    }
}

bool mqtt_manager_is_production(void)
{
    if (mqtt_state != MQTT_STATE_RUNNING)
    {
        return false;
    }
    return s_is_provisioned;
}

/*
    MQTT 恢复出厂设置
 */
void mqtt_manager_factory_reset(void)
{
    ESP_LOGW(TAG, "MQTT factory reset");

    // 1. 停止 MQTT
    mqtt_manager_stop();

    // 2. 销毁客户端
    mqtt_manager_destroy();

    // 3. 清除 NVS 里的 MQTT 配置
    mqtt_config_clear();

    ESP_LOGI(TAG, "MQTT config cleared");
}
// 回调注册
void mqtt_manager_register_callback(mqtt_rx_callback_t callback)
{
    rx_callback = callback;
}

void mqtt_manager_register_status_callback(mqtt_status_callback_t callback)
{
    status_callback = callback;
}