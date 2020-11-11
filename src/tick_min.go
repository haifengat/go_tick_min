package src

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql"

	mapset "github.com/deckarep/golang-set"
	_ "github.com/lib/pq"
	logger "github.com/sirupsen/logrus"
)

var (
	// tick 文件路径
	tickCsvPath string
	// 交易日历
	tradingDays []string
	// 品种交易分钟
	procMins map[string]mins
	// 所有品种的开,收,交易分钟
	beginMins, endMins, tradMins mapset.Set
	// postgres配置
	pgConfig string
)

// mins 品种交易分钟
type mins struct {
	Opens []string
	Ends  []string
	Mins  []string
}

// Bar K线
type Bar struct {
	DateTime     string
	Open         float64
	High         float64
	Low          float64
	Close        float64
	Volume       int64
	OpenInterest float64
}

type Bars []*Bar

func (b Bars) Len() int {
	return len(b)
}
func (b Bars) Less(i, j int) bool {
	return b[i].DateTime < b[j].DateTime
}
func (b Bars) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func init() {
	// 变量初始化
	procMins = make(map[string]mins)
	beginMins = mapset.NewSet()
	endMins = mapset.NewSet()
	tradMins = mapset.NewSet()

	tickCsvPath = "/csv"
	if tmp := os.Getenv("tickCsvPath"); tmp != "" {
		tickCsvPath = tmp
	}

	pgConfig = "postgres://postgres:123456@172.19.129.98:25432/postgres?sslmode=disable"
	if tmp := os.Getenv("pgConfig"); len(tmp) > 0 {
		pgConfig = tmp
	}
	logger.Info("postgres :", pgConfig)

	readCalendar()
	readTradingTime()
}

// readCalendar 取交易日历
func readCalendar() {
	cal, err := os.Open("calendar.csv")
	defer cal.Close()
	if err != nil {
		logger.Error(err)
	}
	reader := csv.NewReader(cal)
	lines, err := reader.ReadAll()
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		if line[1] == "true" {
			tradingDays = append(tradingDays, line[0])
		}
	}
	sort.Strings(tradingDays)
}

// readTradingTime 取交易分钟
func readTradingTime() {
	tt, err := os.Open("tradingtime.csv")
	defer tt.Close()
	if err != nil {
		logger.Error(err)
	}
	reader := csv.NewReader(tt)
	lines, err := reader.ReadAll()
	procDate := make(map[string]string)
	procTime := make(map[string]string)
	for idx, line := range lines {
		// 跳过首行标题
		if len(line) == 0 || idx == 0 {
			continue
		}
		proc, opendate, worktime := line[0], line[1], line[2]
		// 不存在,或时间更新(取最后时间的配置)
		if odate, ok := procDate[proc]; !ok || opendate > odate {
			procTime[proc] = worktime
		}
	}
	// 读取品种交易时间
	type timeSections struct {
		Begin   string `json:"Begin"`
		End     string `json:"End"`
		IsOpen  bool   `json:"IsOpen"`
		IsNight bool   `json:"IsNight"`
		IsClose bool   `json:"IsClose"`
	}

	for proc, v := range procTime {
		var sections []timeSections
		json.Unmarshal([]byte(v), &sections)
		pm := mins{}
		timeFormat := "15:04:05"
		for _, section := range sections {
			// 交易区间的分钟
			b, _ := time.Parse(timeFormat, section.Begin)
			e, _ := time.Parse(timeFormat, section.End)
			pm.Opens = append(pm.Opens, b.Add(-1*time.Minute).Format(timeFormat))
			pm.Ends = append(pm.Ends, section.End)
			// 夜盘过夜
			if e.Before(b) {
				e = e.AddDate(0, 0, 1)
			}
			// 保存小节内分钟
			for i := b; i.Before(e); i = i.Add(time.Minute) {
				pm.Mins = append(pm.Mins, i.Format(timeFormat))
			}
		}
		// 品种对应的分钟数据
		procMins[proc] = pm
		for _, v := range pm.Opens {
			beginMins.Add(v)
		}
		for _, v := range pm.Ends {
			endMins.Add(v)
		}
		for _, v := range pm.Mins {
			tradMins.Add(v)
		}
	}
	// logger.Info("begin:", beginMins, "\nend:", endMins, "\n trad:", tradMins)
}

// Run 根据起码日期执行
func Run(startDay string) {
	// 取最大已处理日期
	if startDay == "" {
		db, err := sql.Open("postgres", pgConfig)
		if err != nil {
			logger.Fatal("数据库打开错误", err)
			return
		}
		defer db.Close()
		rows, err := db.Query(`select max("TradingDay" ) from future.future_min`)
		if err != nil {
			logger.Fatal("取最大交易日报错", err)
		}
		defer rows.Close()
		rows.Next()
		rows.Scan(&startDay)
	} else { // 客户输入日期,处理时包含此值
		// tick 文件列表
		tickFiles := []string{}
		files, _ := ioutil.ReadDir(tickCsvPath)
		for _, f := range files {
			if !f.IsDir() {
				name := strings.Split(f.Name(), ".")[0]
				if name >= startDay {
					tickFiles = append(tickFiles, name)
				}
			}
		}
		sort.Strings(tickFiles)
		startDay = tickFiles[len(tickFiles)-1]

		// 使用chan 控制协程总数
		var waitGroup sync.WaitGroup
		chDay := make(chan string, runtime.NumCPU())
		for _, day := range tickFiles {
			chDay <- day
			waitGroup.Add(1)
			// gzip 读取tick.csv.gz数据
			// 文件不存在,sleep 10min重读
			go func(d string) {
				logger.Infof("%s starting...", d)
				msg, err := RunOnce(d)
				if err != nil {
					logger.Error(msg, err)
					panic(err)
				}
				<-chDay
				waitGroup.Done()
			}(day)
		}
		waitGroup.Wait()
		close(chDay)
	}
	// 取大于日期的交易日
	var days []string
	for _, day := range tradingDays {
		if day > startDay {
			days = append(days, day)
		}
	}

	for _, day := range days {
		// gzip 读取tick.csv.gz数据
		// 文件不存在,sleep 10min重读
		logger.Infof("%s starting...", day)
		for {
			msg, err := RunOnce(day)
			if err == nil {
				break
			} else if msg == "文件不存在" {
				time.Sleep(10 * time.Minute)
				continue
			}
			logger.Error(msg, err)
			return
		}
	}
}

// RunOnce 处理一天数据
func RunOnce(tradingDay string) (string, error) {
	tickFile := path.Join(tickCsvPath, tradingDay+".csv.gz")
	_, err := os.Stat(tickFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "文件不存在", err
		}
		return "取文件状态错误", err
	}
	f, err := os.OpenFile(tickFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return "读取tick csv 失败!", err
	}
	defer f.Close()

	_, err = f.Seek(0, 0) // 切换到文件开始,否则err==EOF
	gr, err := gzip.NewReader(f)
	defer gr.Close()
	if err != nil {
		return "csv.gz读取失败", err
	}

	r := csv.NewReader(gr)

	r.Read() // 首行标题过滤

	// 合约与分钟K线
	instBars := make(map[string]Bars, 0)
	for {
		// TradingDay InstrumentID UpdateTime UpdateMillisec ActionDay LowerLimitPrice UpperLimitPrice BidPrice1 AskPrice1 AskVolume1 BidVolume1 LastPrice Volume OpenInterest Turnover AveragePrice
		row, err := r.Read()
		if err != nil && err != io.EOF {
			logger.Errorf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			break
		}
		// 数据处理
		InstrumentID, UpdateTime, ActionDay, LastPrice, Volume, OpenInterest := row[1], row[2], row[4], row[11], row[12], row[13]
		last, _ := strconv.ParseFloat(LastPrice, 32)
		volume, _ := strconv.ParseInt(Volume, 10, 32)
		oi, _ := strconv.ParseFloat(OpenInterest, 32)

		minTime := UpdateTime[0:6] + "00"
		// 取合约对应的bars
		bars, ok := instBars[InstrumentID]
		if !ok {
			bars = Bars{}
			instBars[InstrumentID] = bars
		}
		// 取当前 bar
		curBar := &Bar{}
		if len(bars) > 0 {
			curBar = bars[len(bars)-1]
		}
		// 当前 日期+时间 yyyyMMddHH:mm:00
		curDt := ActionDay + minTime
		// 新 bar
		if curBar.DateTime != curDt {
			curBar = &Bar{
				DateTime:     curDt,
				Open:         last,
				High:         last,
				Low:          last,
				Close:        last,
				Volume:       volume,
				OpenInterest: oi,
			}
			bars = append(bars, curBar)
			instBars[InstrumentID] = bars
		} else { // 更新现有 bar
			curBar.High = math.Max(curBar.High, last)
			curBar.Low = math.Min(curBar.Low, last)
			curBar.Close = last
			curBar.OpenInterest = oi
			curBar.Volume = volume // 保存 volume 写文件时再处理
		}
	}
	// 分钟数据写入csv
	// DateTime\tInstrument\tOpen\tHigh\tLow\tClose\tVolume\tOpenInterest\n
	sqlStr := `INSERT INTO future.future_min ("DateTime", "Instrument", "Open", "High", "Low", "Close", "Volume", "OpenInterest", "TradingDay") VALUES('%s', '%s', %.4f, %.4f, %.4f, %.4f, %d, %.4f, '%s');`
	db, err := sql.Open("postgres", pgConfig)
	if err != nil {
		return "数据库打开错误", err
	}
	// 退出时关闭
	defer db.Close()

	res, _ := db.Exec(`delete from future.future_min where "TradingDay" = $1`, tradingDay)
	cnt, err := res.RowsAffected()
	if err != nil {
		return "删除当日文件时报错", err
	}
	if cnt > 0 {
		logger.Info("delete data of ", tradingDay, " rows:", strconv.FormatInt(cnt, 10))
	}
	// 开启入库事务
	tx, _ := db.Begin()
	for inst, bars := range instBars {
		// 按datetime排序
		sort.Sort(bars)
		preVol := int64(0)
		for _, bar := range bars {
			// 处理开收盘时间数据
			if bar.High == bar.Low {
				// yyyyMMddHH:mm:00
				barTime := bar.DateTime[8:]
				if beginMins.Contains(barTime) {
					continue
				} else if endMins.Contains(barTime) {
					continue
				} else if !tradMins.Contains(barTime) {
					continue
				}
			}
			t, err := time.Parse("2006010215:04:05", bar.DateTime)
			if err != nil {
				return "时间格式错误!", err
			}
			vol := bar.Volume - preVol
			if vol == 0 {
				continue
			}
			s := fmt.Sprintf(sqlStr, t.Format("2006-01-02 15:04:05"), inst, bar.Open, bar.High, bar.Low, bar.Close, vol, bar.OpenInterest, tradingDay)
			tx.Exec(s)
			preVol = bar.Volume
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return "入库错误", err
	}
	logger.Info(tradingDay, " finished.")
	return "", nil
}
