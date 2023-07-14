package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"io"
	"math"
	"net/http"
	url_pkg "net/url"
	"strconv"
	"time"
)

type VMAdapter struct {
	serverAddress string
	isPrometheus  bool
	logger        logr.Logger
	httpClient    *http.Client
}

type VMStorageAdapter interface {
	ExecuteVictoriaQuery(ctx context.Context, query string, customHeaders map[string]string, ignoreNullValues bool, metricName string, accountID int) (float64, error)
}

func NewVMStorageAdapter(serverAddress string, isPrometheus bool, client *http.Client, logger logr.Logger) VMStorageAdapter {
	return &VMAdapter{
		serverAddress: serverAddress,
		isPrometheus:  isPrometheus,
		logger:        logger,
		httpClient:    client,
	}
}

type promQueryResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (a *VMAdapter) ExecuteVictoriaQuery(ctx context.Context, query string, customHeaders map[string]string, ignoreNullValues bool, metricName string, accountID int) (float64, error) {
	t := time.Now().UTC().Format(time.RFC3339)
	queryEscaped := url_pkg.QueryEscape(query)
	accID := fmt.Sprintf("%d", accountID)
	var url string
	if a.isPrometheus {
		url = fmt.Sprintf("%s/api/v1/query?query=%s&time=%s", a.serverAddress, queryEscaped, t)
	} else {
		url = fmt.Sprintf("%s/select/%s/prometheus/api/v1/query?query=%s&time=%s", a.serverAddress, accID, queryEscaped, t)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return -1, err
	}

	for headerName, headerValue := range customHeaders {
		req.Header.Add(headerName, headerValue)
	}

	r, err := a.httpClient.Do(req)
	if err != nil {
		return -1, err
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return -1, err
	}
	_ = r.Body.Close()

	if !(r.StatusCode >= 200 && r.StatusCode <= 299) {
		err := fmt.Errorf("prometheus query api returned error. status: %d response: %a", r.StatusCode, string(b))
		a.logger.Error(err, "prometheus query api returned error")
		return -1, err
	}

	var result promQueryResult
	err = json.Unmarshal(b, &result)
	if err != nil {
		return -1, err
	}

	var v float64 = -1

	// allow for zero element or single element result sets
	if len(result.Data.Result) == 0 {
		if ignoreNullValues {
			return 0, nil
		}
		return -1, fmt.Errorf("prometheus metrics %a target may be lost, the result is empty", metricName)
	} else if len(result.Data.Result) > 1 {
		fmt.Println(result.Data.Result)
		return -1, fmt.Errorf("prometheus query %a returned multiple elements", query)
	}

	valueLen := len(result.Data.Result[0].Value)
	if valueLen == 0 {
		if ignoreNullValues {
			return 0, nil
		}
		return -1, fmt.Errorf("prometheus metrics %a target may be lost, the value list is empty", metricName)
	} else if valueLen < 2 {
		return -1, fmt.Errorf("prometheus query %a didn't return enough values", query)
	}

	val := result.Data.Result[0].Value[1]
	if val != nil {
		str := val.(string)
		v, err = strconv.ParseFloat(str, 64)
		if err != nil {
			a.logger.Error(err, "Error converting prometheus value", "prometheus_value", str)
			return -1, err
		}
	}

	if math.IsInf(v, 0) {
		if ignoreNullValues {
			return 0, nil
		}
		err := fmt.Errorf("promtheus query returns %f", v)
		a.logger.Error(err, "Error converting prometheus value")
		return -1, err
	}

	return v, nil
}
