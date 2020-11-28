package go_logger

import (
	"errors"
	"github.com/phachon/go-logger/utils"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const FILE_ADAPTER_NAME = "file"

const (
	FILE_SLICE_DATE_NULL  = ""
	FILE_SLICE_DATE_YEAR  = "y"
	FILE_SLICE_DATE_MONTH = "m"
	FILE_SLICE_DATE_DAY   = "d"
	FILE_SLICE_DATE_HOUR  = "h"
)

const (
	FILE_ACCESS_LEVEL = 1000
)

// adapter file
type AdapterFile struct {
	write  map[int]*FileWriter
	config *FileConfig
}

// file writer
type FileWriter struct {
	lock      sync.RWMutex
	writer    *os.File
	startLine int64
	startTime int64
	filename  string
}

func NewFileWrite(fn string) *FileWriter {
	return &FileWriter{
		filename: fn,
	}
}

// file config
type FileConfig struct {

	// log filename
	Filename string

	// level log filename
	LevelFileName map[int]string

	// max file size
	MaxSize int64

	// max file line
	MaxLine int64

	// max file bak
	MaxBak int64

	// file slice by date
	// "y" Log files are cut through year
	// "m" Log files are cut through mouth
	// "d" Log files are cut through day
	// "h" Log files are cut through hour
	DateSlice string

	// is json format
	JsonFormat bool

	// jsonFormat is false, please input format string
	// if format is empty, default format "%millisecond_format% [%level_string%] %body%"
	//
	//  Timestamp "%timestamp%"
	//	TimestampFormat "%timestamp_format%"
	//	Millisecond "%millisecond%"
	//	MillisecondFormat "%millisecond_format%"
	//	Level int "%level%"
	//	LevelString "%level_string%"
	//	Body string "%body%"
	//	File string "%file%"
	//	Line int "%line%"
	//	Function "%function%"
	//
	// example: format = "%millisecond_format% [%level_string%] %body%"
	Format string
}

func (fc *FileConfig) Name() string {
	return FILE_ADAPTER_NAME
}

var fileSliceDateMapping = map[string]int{
	FILE_SLICE_DATE_YEAR:  0,
	FILE_SLICE_DATE_MONTH: 1,
	FILE_SLICE_DATE_DAY:   2,
	FILE_SLICE_DATE_HOUR:  3,
}

func NewAdapterFile() LoggerAbstract {
	return &AdapterFile{
		write:  map[int]*FileWriter{},
		config: &FileConfig{},
	}
}

// init
func (adapterFile *AdapterFile) Init(fileConfig Config) error {
	if fileConfig.Name() != FILE_ADAPTER_NAME {
		return errors.New("logger file adapter init error, config must FileConfig")
	}

	vc := reflect.ValueOf(fileConfig)
	fc := vc.Interface().(*FileConfig)
	adapterFile.config = fc

	if fc.JsonFormat == false && fc.Format == "" {
		fc.Format = defaultLoggerMessageFormat
	}

	if len(adapterFile.config.LevelFileName) == 0 {
		if adapterFile.config.Filename == "" {
			return errors.New("config Filename can't be empty!")
		}
	}
	_, ok := fileSliceDateMapping[adapterFile.config.DateSlice]
	if !ok {
		return errors.New("config DateSlice must be one of the 'y', 'd', 'm','h'!")
	}

	// init FileWriter
	if len(adapterFile.config.LevelFileName) > 0 {
		fileWriters := map[int]*FileWriter{}
		for level, filename := range adapterFile.config.LevelFileName {
			_, ok := levelStringMapping[level]
			if !ok {
				return errors.New("config LevelFileName key level is illegal!")
			}
			fw := NewFileWrite(filename)
			fw.initFile()
			fileWriters[level] = fw
		}
		adapterFile.write = fileWriters
	}

	if adapterFile.config.Filename != "" {
		fw := NewFileWrite(adapterFile.config.Filename)
		fw.initFile()
		adapterFile.write[FILE_ACCESS_LEVEL] = fw
	}

	return nil
}

// Write
func (adapterFile *AdapterFile) Write(loggerMsg *loggerMessage) error {

	var accessChan = make(chan error, 1)
	var levelChan = make(chan error, 1)

	// access file write
	if adapterFile.config.Filename != "" {
		go func() {
			accessFileWrite, ok := adapterFile.write[FILE_ACCESS_LEVEL]
			if !ok {
				accessChan <- nil
				return
			}
			err := accessFileWrite.writeByConfig(adapterFile.config, loggerMsg)
			if err != nil {
				accessChan <- err
				return
			}
			accessChan <- nil
		}()
	}

	// level file write
	if len(adapterFile.config.LevelFileName) != 0 {
		go func() {
			fileWrite, ok := adapterFile.write[loggerMsg.Level]
			if !ok {
				levelChan <- nil
				return
			}
			err := fileWrite.writeByConfig(adapterFile.config, loggerMsg)
			if err != nil {
				levelChan <- err
				return
			}
			levelChan <- nil
		}()
	}

	var accessErr error
	var levelErr error
	if adapterFile.config.Filename != "" {
		accessErr = <-accessChan
	}
	if len(adapterFile.config.LevelFileName) != 0 {
		levelErr = <-levelChan
	}
	if accessErr != nil {
		return accessErr.(error)
	}
	if levelErr != nil {
		return levelErr.(error)
	}
	return nil
}

// Flush
func (adapterFile *AdapterFile) Flush() {
	for _, fileWrite := range adapterFile.write {
		fileWrite.writer.Close()
	}
}

// Name
func (adapterFile *AdapterFile) Name() string {
	return FILE_ADAPTER_NAME
}

// init file
func (fw *FileWriter) initFile() error {

	//check file exits, otherwise create a file
	ok, _ := utils.UtilFile.PathExists(fw.filename)
	if ok == false {
		err := utils.UtilFile.CreateFile(fw.filename)
		if err != nil {
			return err
		}
	}

	// get start time
	fw.startTime = time.Now().Unix()

	// get file start lines
	nowLines, err := utils.UtilFile.GetFileLines(fw.filename)
	if err != nil {
		return err
	}
	fw.startLine = nowLines

	//get a file pointer
	file, err := fw.getFileObject(fw.filename)
	if err != nil {
		return err
	}
	fw.writer = file
	return nil
}

// write by config
func (fw *FileWriter) writeByConfig(config *FileConfig, loggerMsg *loggerMessage) error {

	fw.lock.Lock()
	defer fw.lock.Unlock()

	if config.DateSlice != "" {
		// file slice by date
		err := fw.sliceByDate(config.DateSlice, config.MaxBak)
		if err != nil {
			return err
		}
	}
	if config.MaxLine != 0 {
		// file slice by line
		err := fw.sliceByFileLines(config.MaxLine, config.MaxBak)
		if err != nil {
			return err
		}
	}
	if config.MaxSize != 0 {
		// file slice by size
		err := fw.sliceByFileSize(config.MaxSize, config.MaxBak)
		if err != nil {
			return err
		}
	}

	msg := ""
	if config.JsonFormat == true {
		//jsonByte, _ := json.Marshal(loggerMsg)
		jsonByte, _ := loggerMsg.MarshalJSON()
		msg = string(jsonByte) + "\r\n"
	} else {
		msg = loggerMessageFormat(config.Format, loggerMsg) + "\r\n"
	}

	fw.writer.Write([]byte(msg))
	if config.MaxLine != 0 {
		if config.JsonFormat == true {
			fw.startLine += 1
		} else {
			fw.startLine += int64(strings.Count(msg, "\n"))
		}
	}
	return nil
}

//slice file by date (y, m, d, h, i, s), rename file is file_time.log and recreate file
func (fw *FileWriter) sliceByDate(dataSlice string, maxBak int64) error {

	filename := fw.filename
	filenameSuffix := path.Ext(filename)
	startTime := time.Unix(fw.startTime, 0)
	nowTime := time.Now()

	oldFilename := ""
	isHaveSlice := false
	timeFormat := ""
	if (dataSlice == FILE_SLICE_DATE_YEAR) &&
		(startTime.Year() != nowTime.Year()) {
		isHaveSlice = true
		timeFormat = "2006"
		oldFilename = strings.Replace(filename, filenameSuffix, "", 1) + "_" + startTime.Format(timeFormat) + filenameSuffix
	}
	if (dataSlice == FILE_SLICE_DATE_MONTH) &&
		(startTime.Format("200601") != nowTime.Format("200601")) {
		isHaveSlice = true
		timeFormat = "200601"
		oldFilename = strings.Replace(filename, filenameSuffix, "", 1) + "_" + startTime.Format(timeFormat) + filenameSuffix
	}
	if (dataSlice == FILE_SLICE_DATE_DAY) &&
		(startTime.Format("20060102") != nowTime.Format("20060102")) {
		isHaveSlice = true
		timeFormat = "20060102"
		oldFilename = strings.Replace(filename, filenameSuffix, "", 1) + "_" + startTime.Format(timeFormat) + filenameSuffix
	}
	if (dataSlice == FILE_SLICE_DATE_HOUR) &&
		(startTime.Format("2006010215") != startTime.Format("2006010215")) {
		isHaveSlice = true
		timeFormat = "2006010215"
		oldFilename = strings.Replace(filename, filenameSuffix, "", 1) + "_" + startTime.Format(timeFormat) + filenameSuffix
	}

	if isHaveSlice == true {

		// check bak num
		if maxBak > 0 {
			err := fw.cleanUpBackupFiles(maxBak, timeFormat)
			if err != nil {
				return err
			}
		}

		//close file handle
		fw.writer.Close()
		err := os.Rename(fw.filename, oldFilename)
		if err != nil {
			return err
		}
		err = fw.initFile()
		if err != nil {
			return err
		}
	}

	return nil
}

//slice file by line, if maxLine < fileLine, rename file is file_line_maxLine_time.log and recreate file
func (fw *FileWriter) sliceByFileLines(maxLine int64, maxBak int64) error {

	filename := fw.filename
	filenameSuffix := path.Ext(filename)
	startLine := fw.startLine
	timeFormat := "2006-01-02-15.04.05.9999"

	if startLine >= maxLine {

		// check bak num
		if maxBak > 0 {
			err := fw.cleanUpBackupFiles(maxBak, timeFormat)
			if err != nil {
				return err
			}
		}

		//close file handle
		fw.writer.Close()
		timeFlag := time.Now().Format(timeFormat)
		oldFilename := strings.Replace(filename, filenameSuffix, "", 1) + "." + timeFlag + filenameSuffix
		err := os.Rename(filename, oldFilename)
		if err != nil {
			return err
		}
		err = fw.initFile()
		if err != nil {
			return err
		}
	}

	return nil
}

//slice file by size, if maxSize < fileSize, rename file is file_size_maxSize_time.log and recreate file
func (fw *FileWriter) sliceByFileSize(maxSize int64, maxBak int64) error {

	filename := fw.filename
	filenameSuffix := path.Ext(filename)
	nowSize, _ := fw.getFileSize(filename)
	timeFormat := "2006-01-02-15.04.05.9999"

	if nowSize >= maxSize {
		// check bak num
		if maxBak > 0 {
			err := fw.cleanUpBackupFiles(maxBak, timeFormat)
			if err != nil {
				return err
			}
		}

		//close file handle
		fw.writer.Close()
		timeFlag := time.Now().Format(timeFormat)
		oldFilename := strings.Replace(filename, filenameSuffix, "", 1) + "." + timeFlag + filenameSuffix
		err := os.Rename(filename, oldFilename)
		if err != nil {
			return err
		}
		err = fw.initFile()
		if err != nil {
			return err
		}
	}

	return nil
}

//clean up backup files
//params : maxBak int64, timeFormat string
//return : error
func (fw *FileWriter) cleanUpBackupFiles(maxBak int64, timeFormat string) error {
	filename := fw.filename
	filenameSuffix := path.Ext(filename)

	dirPath, oldFilename := path.Split(filename)
	oldFilename = strings.Replace(oldFilename, filenameSuffix, "", 1)

	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return err
	}

	p := ""
	fileConnect := ""
	switch timeFormat {
	case "2006-01-02-15.04.05.9999":
		p = "[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{0,4}"
		fileConnect = "."
	case "2006":
		p = "[0-9]{4}"
		fileConnect = "_"
	case "200601":
		p = "[0-9]{6}"
		fileConnect = "_"
	case "20060102":
		p = "[0-9]{8}"
		fileConnect = "_"
	case "2006010215":
		p = "[0-9]{10}"
		fileConnect = "_"
	}

	if p == "" {
		return errors.New("time format can not switch expr")
	}

	r, _ := regexp.Compile(p)

	bakFileMap := make(map[int]string)
	bakTimeSlice := make([]int, 0, maxBak)
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}
		match, err := regexp.MatchString(oldFilename+fileConnect+p+filenameSuffix, fi.Name())
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		matchStr := r.FindString(fi.Name())
		if matchStr == "" {
			continue
		}
		t, _ := time.Parse(timeFormat, matchStr)
		bakFileMap[int(t.Unix())] = fi.Name()
		bakTimeSlice = append(bakTimeSlice, int(t.Unix()))
	}

	if int64(len(bakTimeSlice)) < maxBak {
		return nil
	}

	sort.Ints(bakTimeSlice)

	for _, bakTime := range bakTimeSlice[:int64(len(bakTimeSlice))-maxBak+1] {
		err := os.Remove(bakFileMap[bakTime])
		if err != nil {
			return err
		}
	}

	return nil
}

//get file object
//params : filename
//return : *os.file, error
func (fw *FileWriter) getFileObject(filename string) (file *os.File, err error) {
	file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0766)
	return file, err
}

//get file size
//params : filename
//return : fileSize(byte int64), error
func (fw *FileWriter) getFileSize(filename string) (fileSize int64, err error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return fileSize, err
	}

	return fileInfo.Size() / 1024, nil
}

func init() {
	Register(FILE_ADAPTER_NAME, NewAdapterFile)
}
