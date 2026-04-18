package logic

import "fmt"

// ---------------------------------------------------------------------------
// Logic 公共定义
// ---------------------------------------------------------------------------

// errSymbolRequired symbol 必填错误
var errSymbolRequired = fmt.Errorf("symbol is required")

const defaultSymbol = "ETHUSDT"

func normalizeSymbol(symbol string) string {
	if symbol == "" {
		return defaultSymbol
	}
	return symbol
}
