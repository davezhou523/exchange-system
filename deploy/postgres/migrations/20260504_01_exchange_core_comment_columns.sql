-- 为已存在的 exchange_core 表补充表备注与字段备注。
-- 该脚本用于增量迁移，不替代初始化脚本。

COMMENT ON TABLE exchange_core.strategy_template IS '策略模板主表';
COMMENT ON COLUMN exchange_core.strategy_template.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.strategy_template.template_code IS '模板编码';
COMMENT ON COLUMN exchange_core.strategy_template.template_name IS '模板名称';
COMMENT ON COLUMN exchange_core.strategy_template.bucket IS '模板所属分桶';
COMMENT ON COLUMN exchange_core.strategy_template.enabled IS '模板是否启用';
COMMENT ON COLUMN exchange_core.strategy_template.description IS '模板描述';
COMMENT ON COLUMN exchange_core.strategy_template.created_at IS '创建时间';
COMMENT ON COLUMN exchange_core.strategy_template.updated_at IS '更新时间';

COMMENT ON TABLE exchange_core.strategy_param_version IS '策略参数版本表';
COMMENT ON COLUMN exchange_core.strategy_param_version.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.strategy_param_version.template_code IS '关联模板编码';
COMMENT ON COLUMN exchange_core.strategy_param_version.version IS '参数版本号';
COMMENT ON COLUMN exchange_core.strategy_param_version.params_json IS '策略参数 JSON';
COMMENT ON COLUMN exchange_core.strategy_param_version.published_by IS '发布时间操作人';
COMMENT ON COLUMN exchange_core.strategy_param_version.published_at IS '发布时间';
COMMENT ON COLUMN exchange_core.strategy_param_version.remark IS '版本备注';
COMMENT ON COLUMN exchange_core.strategy_param_version.created_at IS '创建时间';
COMMENT ON COLUMN exchange_core.strategy_param_version.updated_at IS '更新时间';

COMMENT ON TABLE exchange_core.orders IS '订单主表';
COMMENT ON COLUMN exchange_core.orders.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.orders.account_id IS '账户标识';
COMMENT ON COLUMN exchange_core.orders.symbol IS '交易对标识';
COMMENT ON COLUMN exchange_core.orders.order_id IS '交易所订单标识';
COMMENT ON COLUMN exchange_core.orders.client_order_id IS '客户端订单标识';
COMMENT ON COLUMN exchange_core.orders.strategy_id IS '策略标识';
COMMENT ON COLUMN exchange_core.orders.position_cycle_id IS '仓位周期标识';
COMMENT ON COLUMN exchange_core.orders.side IS '买卖方向';
COMMENT ON COLUMN exchange_core.orders.type IS '订单类型';
COMMENT ON COLUMN exchange_core.orders.price IS '委托价格';
COMMENT ON COLUMN exchange_core.orders.quantity IS '委托数量';
COMMENT ON COLUMN exchange_core.orders.executed_quantity IS '累计成交数量';
COMMENT ON COLUMN exchange_core.orders.status IS '订单状态';
COMMENT ON COLUMN exchange_core.orders.reduce_only IS '是否仅减仓';
COMMENT ON COLUMN exchange_core.orders.created_at IS '创建时间';
COMMENT ON COLUMN exchange_core.orders.updated_at IS '更新时间';

COMMENT ON TABLE exchange_core.trades IS '成交明细表';
COMMENT ON COLUMN exchange_core.trades.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.trades.account_id IS '账户标识';
COMMENT ON COLUMN exchange_core.trades.symbol IS '交易对标识';
COMMENT ON COLUMN exchange_core.trades.trade_id IS '成交标识';
COMMENT ON COLUMN exchange_core.trades.order_id IS '关联订单标识';
COMMENT ON COLUMN exchange_core.trades.price IS '成交价格';
COMMENT ON COLUMN exchange_core.trades.qty IS '成交数量';
COMMENT ON COLUMN exchange_core.trades.fee IS '手续费';
COMMENT ON COLUMN exchange_core.trades.fee_asset IS '手续费资产';
COMMENT ON COLUMN exchange_core.trades.realized_pnl IS '已实现盈亏';
COMMENT ON COLUMN exchange_core.trades.trade_time IS '成交时间';
COMMENT ON COLUMN exchange_core.trades.created_at IS '创建时间';

COMMENT ON TABLE exchange_core.positions IS '持仓快照表';
COMMENT ON COLUMN exchange_core.positions.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.positions.account_id IS '账户标识';
COMMENT ON COLUMN exchange_core.positions.symbol IS '交易对标识';
COMMENT ON COLUMN exchange_core.positions.position_side IS '持仓方向';
COMMENT ON COLUMN exchange_core.positions.position_amt IS '持仓数量';
COMMENT ON COLUMN exchange_core.positions.entry_price IS '开仓均价';
COMMENT ON COLUMN exchange_core.positions.mark_price IS '标记价格';
COMMENT ON COLUMN exchange_core.positions.unrealized_pnl IS '未实现盈亏';
COMMENT ON COLUMN exchange_core.positions.leverage IS '杠杆倍数';
COMMENT ON COLUMN exchange_core.positions.updated_at IS '更新时间';

COMMENT ON TABLE exchange_core.api_audit_log IS 'API 审计日志表';
COMMENT ON COLUMN exchange_core.api_audit_log.id IS '主键 ID';
COMMENT ON COLUMN exchange_core.api_audit_log.request_id IS '请求链路标识';
COMMENT ON COLUMN exchange_core.api_audit_log.user_id IS '用户标识';
COMMENT ON COLUMN exchange_core.api_audit_log.account_id IS '账户标识';
COMMENT ON COLUMN exchange_core.api_audit_log.path IS '请求路径';
COMMENT ON COLUMN exchange_core.api_audit_log.method IS '请求方法';
COMMENT ON COLUMN exchange_core.api_audit_log.action IS '业务动作';
COMMENT ON COLUMN exchange_core.api_audit_log.request_body_json IS '请求体 JSON';
COMMENT ON COLUMN exchange_core.api_audit_log.response_code IS '响应状态码';
COMMENT ON COLUMN exchange_core.api_audit_log.ip IS '请求来源 IP';
COMMENT ON COLUMN exchange_core.api_audit_log.created_at IS '创建时间';