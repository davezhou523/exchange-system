# 交易系统撮合、订单、资金、清算、风控
### 
# 在项目根目录执行
goctl rpc protoc rpc/matching/matching.proto \
--go_out=rpc/matching \
--go-grpc_out=rpc/matching \
--zrpc_out=rpc/matching \
--style=goZero


goctl api go -api exchange.api -dir . -style goZero