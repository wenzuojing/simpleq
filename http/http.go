package http

import (
	"fmt"
	"github.com/wenzuojing/simpleq/broker"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
)

var publishR *regexp.Regexp = regexp.MustCompile("/publish/([^/]+)")

var consumeR *regexp.Regexp = regexp.MustCompile("^/consume/([^/]+)(/([^/]+))?$")

type httpHandle struct {
	broker *broker.Broker
}

func (h httpHandle) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	defer func() {
		x := recover()
		if x != nil {
			log.Println("[error]", x)
			res.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(res, "error:", x)
		}
	}()

	uri := req.RequestURI

	method := req.Method

	if "PUT" == method || "POST" == method {
		bb := publishR.FindSubmatch([]byte(uri))
		if len(bb) != 0 {
			if mesage, err := ioutil.ReadAll(req.Body); err != nil {
				panic(err)
			} else {
				if err := h.broker.Write(bb[1], mesage); err != nil {
					panic(err)
				} else {
					res.Write([]byte("ok"))
					return
				}
			}
		}
	}

	if "GET" == method {
		bb := consumeR.FindSubmatch([]byte(uri))
		if len(bb) != 0 {
			if data, err := h.broker.Read(bb[1], bb[3], 1); err != nil {
				panic(err)
			} else {
				if data != nil && len(data) == 1 {
					res.Write(data[0])
				}
				return
			}

		}
	}

	res.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(res, "error:unknow handle")

}

func StartHttpServer(host string, port int) error {

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), httpHandle{broker: broker.NewBroker("/home/wens/data")})

	if err != nil {
		return err
	}

	return nil

}
