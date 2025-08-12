package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type PrometheusClient struct {
	serverURL string
	client    *http.Client
}

type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func NewPrometheusClient(serverURL string) *PrometheusClient {
	return &PrometheusClient{
		serverURL: serverURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (pc *PrometheusClient) GetMetricValue(metricName string, labels map[string]string) (float64, error) {
	query := metricName
	if len(labels) > 0 {
		var labelParts []string
		for key, value := range labels {
			labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, key, value))
		}
		query = fmt.Sprintf("%s{%s}", metricName, strings.Join(labelParts, ","))
	}

	url := fmt.Sprintf("%s/api/v1/query?query=%s", pc.serverURL, query)
	
	resp, err := pc.client.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to query Prometheus: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}

	var promResp PrometheusResponse
	if err := json.Unmarshal(body, &promResp); err != nil {
		return 0, fmt.Errorf("failed to parse Prometheus response: %w", err)
	}

	if promResp.Status != "success" {
		return 0, fmt.Errorf("Prometheus query failed: %s", promResp.Status)
	}

	if len(promResp.Data.Result) == 0 {
		return 0, fmt.Errorf("no results found for metric: %s", metricName)
	}

	// 첫 번째 결과의 값을 파싱
	value := promResp.Data.Result[0].Value[1].(string)
	return strconv.ParseFloat(value, 64)
}

func (pc *PrometheusClient) GetBlockTime() (time.Duration, error) {
	value, err := pc.GetMetricValue("cosmos_block_time", nil)
	if err != nil {
		return 0, err
	}
	return time.Duration(value * float64(time.Second)), nil
}

func (pc *PrometheusClient) GetAverageBlockTime() (time.Duration, error) {
	value, err := pc.GetMetricValue("cosmos_avg_block_time", nil)
	if err != nil {
		return 0, err
	}
	return time.Duration(value * float64(time.Second)), nil
}

func (pc *PrometheusClient) GetNodeHeight() (float64, error) {
	return pc.GetMetricValue("cosmos_node_height", nil)
}
