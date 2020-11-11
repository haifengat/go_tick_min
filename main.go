package main

import (
	"flag"
	"fmt"
	"tick-min/src"
)

var (
	singleDay = ""
	startDay  = ""
)

func init() {
	flag.StringVar(&singleDay, "s", "", "处理指定某一天的数据")
	flag.StringVar(&startDay, "m", "", "从指定日期开始处理数据, 默认为空, 取库中最大交易日的下一日处理")
}

func main() {
	flag.Parse()
	if singleDay != "" {
		msg, err := src.RunOnce(singleDay)
		if err != nil {
			_ = fmt.Errorf("%s: %v", msg, err)
		}
	} else {
		src.Run(startDay)
	}
}
