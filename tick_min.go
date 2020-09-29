package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"time"

	logger "github.com/sirupsen/logrus"
)

var (
	// tick 文件路径
	tickCsvPath string
	// 交易日历
	tradingDays []string
	// 品种交易分钟
	procMins map[string]ProcMins
)

// ProcMins 品种交易分钟
type ProcMins struct {
	Opens []string
	Ends  []string
	Mins  []string
}

func init() {
	procMins = make(map[string]ProcMins)
	readCalendar()
	readTradingTime()
}

// 取交易日历
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
		if len(line) == 0 || idx == 0 {
			continue
		}
		proc, opendate, worktime := line[0], line[1], line[2]
		// 不存在,或时间更新
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
		pm := ProcMins{}
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
			mins := make([]string, 0)
			for i := b; i.Before(e); i = i.Add(time.Minute) {
				mins = append(mins, i.Format(timeFormat))
			}
			pm.Mins = mins
		}
		// 品种对应的分钟数据
		procMins[proc] = pm
	}
}

// Run 根据起码日期执行
func Run(startDay string) {
	// 取大于日期的交易日
	var days []string
	for _, day := range tradingDays {
		if day >= startDay {
			days = append(days, day)
		}
	}
	for _, day := range days {
		// gzip 读取tick.csv.gz数据
		// run(day)
		println(day)
	}
}

func run(tradingDay string) (string, error) {
	f, err := os.OpenFile(path.Join(tickCsvPath, tradingDay+".csv.gz"), os.O_RDONLY, os.ModePerm)
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
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			logger.Errorf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			break
		}
		// 数据处理
		fmt.Print(row)
	}

	return "", nil
}
