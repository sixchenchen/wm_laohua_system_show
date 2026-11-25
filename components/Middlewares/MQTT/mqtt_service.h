#ifndef MQTT_SERVICE_H
#define MQTT_SERVICE_H

#include <stdint.h>
#include "mqtt_config.h"

// 初始化 MQTT 服务（使用 NVS 配置）
void mqtt_service_init(void);

// 使用指定配置初始化 MQTT 服务
void mqtt_service_init_with_config(const mqtt_config_t *config);

// 发布 mos 状态
void mqtt_publish_mos_state(void);

// 发布 mos 事件
void mqtt_service_publish_mos_event(uint8_t channel, uint8_t state);

#endif