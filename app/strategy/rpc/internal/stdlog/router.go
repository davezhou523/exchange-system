package stdlog

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RouteRule 定义单条标准日志命中的分类规则。
type RouteRule struct {
	name     string
	patterns []string
}

// Router 按日志内容把标准库 log 输出同时写入控制台和分类文件。
type Router struct {
	mu          sync.Mutex
	stdout      io.Writer
	buffer      bytes.Buffer
	baseDir     string
	currentDate string
	allFile     *os.File
	defaultFile *os.File
	files       map[string]*os.File
	rules       []RouteRule
}

// Setup 初始化 strategy 进程的标准日志分类路由。
func Setup(baseDir string) (io.Closer, error) {
	router, err := newRouter(baseDir)
	if err != nil {
		return nil, err
	}
	log.SetOutput(router)
	log.SetFlags(log.Ldate | log.Ltime)
	return router, nil
}

// newRouter 创建标准日志路由器并打开所需的分类日志文件。
func newRouter(baseDir string) (*Router, error) {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = "logs"
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir %s: %w", baseDir, err)
	}

	router := &Router{
		stdout:  os.Stdout,
		baseDir: baseDir,
		files:   make(map[string]*os.File),
		rules: []RouteRule{
			{name: "strategy.warmup.alert.log", patterns: []string{"restore_behind", "restore_ahead"}},
			{name: "strategy.warmup.restore_count.log", patterns: []string{"event=restore_count"}},
			{name: "strategy.warmup.restore_sync.log", patterns: []string{"event=restore_sync"}},
			{name: "strategy.warmup.log", patterns: []string{"[strategy-warmup]"}},
			{name: "strategy.kafka.log", patterns: []string{"[kafka consume]", "kafka consumer", "kafka consume loop", "consumer-group", "[mock]"}},
			{name: "strategy.decision.log", patterns: []string{"[decision]", "[decision-log]"}},
			{name: "strategy.universe.log", patterns: []string{"[universe]", "[universe-log]"}},
			{name: "strategy.marketstate.log", patterns: []string{"[marketstate-log]"}},
			{name: "strategy.weights.log", patterns: []string{"[weights-log]"}},
			{name: "strategy.clickhouse.log", patterns: []string{"[strategy-clickhouse]"}},
		},
	}
	if err := router.rotateFilesIfNeeded(time.Now()); err != nil {
		_ = router.Close()
		return nil, err
	}
	return router, nil
}

// openLogFile 以追加模式打开单个标准日志目标文件。
func openLogFile(baseDir, dateDir, name string) (*os.File, error) {
	logDir := filepath.Join(baseDir, dateDir)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create dated log dir %s: %w", logDir, err)
	}
	path := filepath.Join(logDir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", path, err)
	}
	return file, nil
}

// Write 接收标准日志写入并按完整行路由到对应分类文件。
func (r *Router) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.rotateFilesIfNeeded(time.Now()); err != nil {
		return 0, err
	}
	if _, err := r.buffer.Write(p); err != nil {
		return 0, err
	}
	for {
		line, ok := r.nextLine()
		if !ok {
			break
		}
		if err := r.routeLine(line); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// rotateFilesIfNeeded 在日期变化时切换到新的按天日志文件。
func (r *Router) rotateFilesIfNeeded(now time.Time) error {
	dateDir := now.UTC().Format("2006-01-02")
	if dateDir == r.currentDate && r.allFile != nil && r.defaultFile != nil {
		return nil
	}

	if err := r.closeFilesLocked(); err != nil {
		return err
	}

	allFile, err := openLogFile(r.baseDir, dateDir, "strategy.all.log")
	if err != nil {
		return err
	}
	defaultFile, err := openLogFile(r.baseDir, dateDir, "strategy.misc.log")
	if err != nil {
		_ = allFile.Close()
		return err
	}

	files := make(map[string]*os.File, len(r.rules))
	for _, rule := range r.rules {
		file, openErr := openLogFile(r.baseDir, dateDir, rule.name)
		if openErr != nil {
			_ = allFile.Close()
			_ = defaultFile.Close()
			for _, opened := range files {
				_ = opened.Close()
			}
			return openErr
		}
		files[rule.name] = file
	}

	r.currentDate = dateDir
	r.allFile = allFile
	r.defaultFile = defaultFile
	r.files = files
	return nil
}

// nextLine 从内部缓冲区取出一整行日志。
func (r *Router) nextLine() ([]byte, bool) {
	data := r.buffer.Bytes()
	index := bytes.IndexByte(data, '\n')
	if index < 0 {
		return nil, false
	}

	line := make([]byte, index+1)
	copy(line, data[:index+1])
	r.buffer.Next(index + 1)
	return line, true
}

// routeLine 把一行日志同时写到控制台、总日志以及命中的分类日志。
func (r *Router) routeLine(line []byte) error {
	if _, err := r.stdout.Write(line); err != nil {
		return err
	}
	if _, err := r.allFile.Write(line); err != nil {
		return err
	}

	target := r.defaultFile
	text := string(line)
	for _, rule := range r.rules {
		if r.matchRule(text, rule) {
			target = r.files[rule.name]
			break
		}
	}
	if target == nil {
		return nil
	}
	_, err := target.Write(line)
	return err
}

// matchRule 判断当前日志行是否命中指定分类规则。
func (r *Router) matchRule(line string, rule RouteRule) bool {
	for _, pattern := range rule.patterns {
		if strings.Contains(line, pattern) {
			return true
		}
	}
	return false
}

// Close 关闭日志路由器持有的全部文件句柄。
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeFilesLocked()
}

// closeFilesLocked 关闭当前日期持有的全部日志文件，调用方必须先持有互斥锁。
func (r *Router) closeFilesLocked() error {
	var firstErr error
	if r.allFile != nil {
		if err := r.allFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.allFile = nil
	}
	if r.defaultFile != nil {
		if err := r.defaultFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.defaultFile = nil
	}
	for name, file := range r.files {
		if file == nil {
			delete(r.files, name)
			continue
		}
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.files, name)
	}
	return firstErr
}
