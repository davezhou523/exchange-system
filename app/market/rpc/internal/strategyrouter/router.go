package strategyrouter

import (
	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/common/regimejudge"
)

// Bucket 表示策略路由输出的策略桶类型，供后续 weights 直接使用。
type Bucket string

const (
	// BucketTrend 表示趋势策略桶。
	BucketTrend Bucket = "trend"
	// BucketBreakout 表示突破策略桶。
	BucketBreakout Bucket = "breakout"
	// BucketRange 表示震荡策略桶。
	BucketRange Bucket = "range"
)

// Config 定义策略路由器所需的最小模板与阈值配置。
type Config struct {
	StaticTemplateMap     map[string]string
	RangeTemplate         string
	BreakoutTemplate      string
	BTCTrendTemplate      string
	BTCTrendAtrPctMax     float64
	HighBetaSafeTemplate  string
	HighBetaSafeSymbols   []string
	HighBetaSafeAtrPct    float64
	HighBetaDisableAtrPct float64
}

// HasEntries 判断当前 Router 配置是否已经显式写入过动态模板或静态模板映射。
func (c Config) HasEntries() bool {
	return len(c.StaticTemplateMap) > 0 ||
		c.RangeTemplate != "" ||
		c.BreakoutTemplate != "" ||
		c.BTCTrendTemplate != "" ||
		c.HighBetaSafeTemplate != "" ||
		len(c.HighBetaSafeSymbols) > 0
}

// Clone 返回一份 Router 配置快照，避免运行时修改影响原始配置。
func (c Config) Clone() Config {
	out := c
	out.StaticTemplateMap = cloneStringMap(c.StaticTemplateMap)
	out.HighBetaSafeSymbols = append([]string(nil), c.HighBetaSafeSymbols...)
	return out
}

// Input 表示单个交易对做模板路由时需要的最小上下文。
type Input struct {
	Symbol         string
	Close          float64
	Atr            float64
	Ema21          float64
	Ema55          float64
	MarketState    marketstate.MarketState
	MarketAnalysis regimejudge.Analysis
}

// Decision 表示一次模板与策略桶路由的结构化结果。
type Decision struct {
	BaseTemplate string
	Template     string
	Bucket       Bucket
	Enabled      bool
	Reason       string
}

const (
	// ReasonHealthyData 表示当前样本通过最小健康门禁，但未发生动态模板切换。
	ReasonHealthyData = "healthy_data"
	// ReasonMarketStateRange 表示当前样本命中震荡路由。
	ReasonMarketStateRange = regimejudge.RouteReasonRange
	// ReasonMarketStateBreakout 表示当前样本命中突破路由。
	ReasonMarketStateBreakout = regimejudge.RouteReasonBreakout
	// ReasonMarketStateTrend 表示当前样本命中趋势路由。
	ReasonMarketStateTrend = regimejudge.RouteReasonTrend
	// ReasonTrendStrong 表示当前样本仅通过兼容趋势 fallback 切到更积极模板。
	ReasonTrendStrong = "trend_strong"
)

// Router 负责把统一 Analysis 映射为具体策略模板和策略桶。
type Router struct {
	cfg Config
}

// New 创建一个带最小默认阈值的策略路由器。
func New(cfg Config) *Router {
	cfg = cfg.Clone()
	if cfg.BTCTrendAtrPctMax <= 0 {
		cfg.BTCTrendAtrPctMax = 0.003
	}
	if cfg.HighBetaSafeAtrPct <= 0 {
		cfg.HighBetaSafeAtrPct = 0.006
	}
	if cfg.HighBetaDisableAtrPct <= 0 {
		cfg.HighBetaDisableAtrPct = 0.012
	}
	return &Router{cfg: cfg}
}

// BaseTemplate 返回某个交易对的基础模板，供 Universe 在无动态切换时回退使用。
func (r *Router) BaseTemplate(symbol string) string {
	if r == nil || r.cfg.StaticTemplateMap == nil {
		return ""
	}
	return r.cfg.StaticTemplateMap[symbol]
}

// Route 根据统一 Analysis 和少量兼容字段输出当前交易对应使用的模板与策略桶。
func (r *Router) Route(in Input) Decision {
	baseTemplate := r.BaseTemplate(in.Symbol)
	out := Decision{
		BaseTemplate: baseTemplate,
		Template:     baseTemplate,
		Bucket:       RouteBucketForTemplate(baseTemplate),
		Enabled:      true,
		Reason:       ReasonHealthyData,
	}
	if r == nil {
		return out
	}
	for _, rule := range []func(Input, Decision) (Decision, bool){
		r.routeHighBetaSafe,
		r.routeRange,
		r.routeBreakout,
		r.routeBTCTrend,
	} {
		if decision, ok := rule(in, out); ok {
			return decision
		}
	}
	return out
}

// routeRange 处理震荡模板切换，优先信任统一 Analysis，只有缺失时才回退旧 MarketState。
func (r *Router) routeRange(in Input, fallback Decision) (Decision, bool) {
	if r == nil || r.cfg.RangeTemplate == "" {
		return Decision{}, false
	}
	ok, reason := shouldRouteRange(in)
	if !ok {
		return Decision{}, false
	}
	fallback.Template = r.cfg.RangeTemplate
	fallback.Bucket = BucketRange
	fallback.Reason = reason
	return fallback, true
}

// routeBreakout 处理突破模板切换，优先复用统一 Analysis 解释，避免路由层再散写判态逻辑。
func (r *Router) routeBreakout(in Input, fallback Decision) (Decision, bool) {
	if r == nil || r.cfg.BreakoutTemplate == "" {
		return Decision{}, false
	}
	ok, reason := shouldRouteBreakout(in)
	if !ok {
		return Decision{}, false
	}
	fallback.Template = r.cfg.BreakoutTemplate
	fallback.Bucket = BucketBreakout
	fallback.Reason = reason
	return fallback, true
}

// routeBTCTrend 处理 BTC 趋势模板切换，把 Analysis 优先、兼容 fallback 次之的口径集中到一处。
func (r *Router) routeBTCTrend(in Input, fallback Decision) (Decision, bool) {
	if r == nil || in.Symbol != "BTCUSDT" || r.cfg.BTCTrendTemplate == "" {
		return Decision{}, false
	}
	ok, reason := r.shouldUseBTCTrend(in)
	if !ok {
		return Decision{}, false
	}
	fallback.Template = r.cfg.BTCTrendTemplate
	fallback.Bucket = BucketTrend
	fallback.Reason = reason
	return fallback, true
}

// routeHighBetaSafe 处理高波动币的保护模板切换，避免这部分规则继续散落在 Universe 里。
func (r *Router) routeHighBetaSafe(in Input, fallback Decision) (Decision, bool) {
	if r == nil || !r.isHighBetaSafeSymbol(in.Symbol) || r.cfg.HighBetaSafeTemplate == "" {
		return Decision{}, false
	}
	atrPct := r.inputAtrPct(in)
	if atrPct >= r.cfg.HighBetaDisableAtrPct {
		fallback.Template = r.cfg.HighBetaSafeTemplate
		fallback.Bucket = BucketTrend
		fallback.Enabled = false
		fallback.Reason = "volatility_extreme"
		return fallback, true
	}
	if atrPct >= r.cfg.HighBetaSafeAtrPct {
		fallback.Template = r.cfg.HighBetaSafeTemplate
		fallback.Bucket = BucketTrend
		fallback.Reason = "volatility_high"
		return fallback, true
	}
	return Decision{}, false
}

// shouldUseBTCTrend 判断 BTC 是否应切换到更积极的趋势模板，并返回对应的统一原因码。
func (r *Router) shouldUseBTCTrend(in Input) (bool, string) {
	atrPct := r.inputAtrPct(in)
	if atrPct <= 0 || atrPct > r.cfg.BTCTrendAtrPctMax {
		return false, ""
	}
	if in.MarketAnalysis.PrefersTrend() {
		return true, in.MarketAnalysis.TrendReason()
	}
	if in.MarketAnalysis.HasSignal() {
		return false, ""
	}
	if in.MarketState == marketstate.MarketStateTrendUp || in.MarketState == marketstate.MarketStateTrendDown {
		return true, ReasonMarketStateTrend
	}
	if (in.Close > in.Ema21 && in.Ema21 > in.Ema55) ||
		(in.Close < in.Ema21 && in.Ema21 < in.Ema55) {
		return true, ReasonTrendStrong
	}
	return false, ""
}

// isHighBetaSafeSymbol 判断某个交易对是否启用了 high-beta-safe 保护规则。
func (r *Router) isHighBetaSafeSymbol(symbol string) bool {
	if r == nil {
		return false
	}
	for _, item := range r.cfg.HighBetaSafeSymbols {
		if item == symbol {
			return true
		}
	}
	return false
}

// inputAtrPct 优先复用 Analysis 已归一化的 AtrPct，避免路由层重复解释同一份特征。
func (r *Router) inputAtrPct(in Input) float64 {
	if in.MarketAnalysis.Features.AtrPct > 0 {
		return in.MarketAnalysis.Features.AtrPct
	}
	if in.Close <= 0 || in.Atr <= 0 {
		return 0
	}
	return in.Atr / in.Close
}

// RouteBucketForTemplate 根据模板名映射出默认策略桶，供 Universe 和 weights 复用同一套桶规则。
func RouteBucketForTemplate(template string) Bucket {
	switch template {
	case "breakout-core":
		return BucketBreakout
	case "range-core":
		return BucketRange
	default:
		return BucketTrend
	}
}

// shouldRouteRange 判断当前输入是否应切到震荡模板；若 Analysis 已存在，则不再继续回退旧状态口径。
func shouldRouteRange(in Input) (bool, string) {
	if reason := in.MarketAnalysis.RangeReason(); reason != "" {
		return true, reason
	}
	if in.MarketAnalysis.HasSignal() {
		return false, ""
	}
	if in.MarketState == marketstate.MarketStateRange {
		return true, ReasonMarketStateRange
	}
	return false, ""
}

// shouldRouteBreakout 判断当前输入是否应切到突破模板；若 Analysis 已存在，则不再继续回退旧状态口径。
func shouldRouteBreakout(in Input) (bool, string) {
	if reason := in.MarketAnalysis.BreakoutReason(); reason != "" {
		return true, reason
	}
	if in.MarketAnalysis.HasSignal() {
		return false, ""
	}
	if in.MarketState == marketstate.MarketStateBreakout {
		return true, ReasonMarketStateBreakout
	}
	return false, ""
}

// cloneStringMap 复制 Router 使用的静态模板映射，避免上下游共享同一底层 map。
func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
