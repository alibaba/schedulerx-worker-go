/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// getFormatIpv4Addr obtain a formatted IPv4 address,
// such as the address of 127.0.0.1, which is formatted as a string of 127000000001.
func getFormatIpv4Addr() (string, error) {
	ipv4Addr, err := GetIpv4AddrHost()
	if err != nil {
		return "", err
	}
	segments := strings.Split(ipv4Addr, ".")
	var result string
	for _, segment := range segments {
		num, _ := strconv.Atoi(segment)
		result += fmt.Sprintf("%03d", num)
	}
	return result, nil
}

func GetIpv4AddrHost() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// check if ip addr is loopback addr
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("cannot find valid ipv4 addr")
}

func ParseIPAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("Invalid addr: %s, err:%s ", addr, err.Error())
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("Invalid addr: %s, err:%s ", addr, err.Error())
	}
	return host, port, nil
}
