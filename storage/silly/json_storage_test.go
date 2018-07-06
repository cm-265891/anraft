package silly

import (
    "os"
	"testing"
)

func TestGetSet(t *testing.T) {
	filename := "silly_data"
	defer os.Remove(filename)

	s := &SillyStorage{}
	s.Init("./")

	tmp, err := s.Get("abc")
	if err != nil {
		t.Errorf("get empty failed:%v", err)
	}
	if tmp != nil {
		t.Errorf("get empty should be nil")
	}

	err = s.Set("abc", []byte("def"))
	if err != nil {
		t.Errorf("set key failed:%v", err)
	}
	tmp, err = s.Get("abc")
	if err != nil {
		t.Errorf("get key failed:%v", err)
	}
	if string(tmp) != "def" {
		t.Errorf("invalid key content")
	}
}
