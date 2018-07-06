package silly

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"io/ioutil"
	"os"
	"sync"
)

type SillyStorage struct {
	filename string
	mutex    sync.Mutex
	// TODO(wolf): flock to guarentee only one process uses it
}

func (s *SillyStorage) Init(path string) {
	// create file if not exists
	s.filename = fmt.Sprintf("%s/%s", path, "silly_data")
	f, err := os.OpenFile(s.filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("open file:%s failed:%v", s.filename, err)
	}
	f.Close()
}

func (s *SillyStorage) byte2string(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func (s *SillyStorage) string2byte(input string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(input)
}

func (s *SillyStorage) Set(key string, val []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	content, err := ioutil.ReadFile(s.filename)
	if err != nil {
		return err
	}
	valstr := s.byte2string(val)
	result := make(map[string]string)
	if len(content) != 0 {
		if err := json.Unmarshal(content, &result); err != nil {
			return err
		}
	}
	result[key] = valstr
	if bytes, err := json.MarshalIndent(result, "", "    "); err != nil {
		return err
	} else {
		return ioutil.WriteFile(s.filename, bytes, 0644)
	}
}

func (s *SillyStorage) Get(key string) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	content, err := ioutil.ReadFile(s.filename)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	if len(content) != 0 {
		if err := json.Unmarshal(content, &result); err != nil {
			return nil, err
		}
	}
	if res, ok := result[key]; !ok {
		return nil, nil
	} else {
		return s.string2byte(res)
	}
}
