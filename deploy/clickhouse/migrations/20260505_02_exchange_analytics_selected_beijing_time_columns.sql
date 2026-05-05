-- 为指定分析事实表补充北京时间别名列，保留原 UTC 字段不变。

ALTER TABLE exchange_analytics.kline_fact
    ADD COLUMN IF NOT EXISTS open_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(open_time, 'Asia/Shanghai')
    COMMENT 'K 线开始时间（北京时间）'
    AFTER open_time;

ALTER TABLE exchange_analytics.decision_fact
    ADD COLUMN IF NOT EXISTS open_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(open_time, 'Asia/Shanghai')
    COMMENT '关联 K 线开始时间（北京时间）'
    AFTER open_time;

ALTER TABLE exchange_analytics.signal_fact
    ADD COLUMN IF NOT EXISTS event_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(event_time, 'Asia/Shanghai')
    COMMENT '信号事件时间（北京时间）'
    AFTER event_time;

ALTER TABLE exchange_analytics.execution_event_fact
    ADD COLUMN IF NOT EXISTS event_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(event_time, 'Asia/Shanghai')
    COMMENT '执行事件时间（北京时间）'
    AFTER event_time;

ALTER TABLE exchange_analytics.order_event_fact
    ADD COLUMN IF NOT EXISTS event_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(event_time, 'Asia/Shanghai')
    COMMENT '订单事件时间（北京时间）'
    AFTER event_time;

ALTER TABLE exchange_analytics.position_cycle_fact
    ADD COLUMN IF NOT EXISTS open_time_bj DateTime64(3, 'Asia/Shanghai')
    ALIAS toTimeZone(open_time, 'Asia/Shanghai')
    COMMENT '仓位周期开始时间（北京时间）'
    AFTER open_time;