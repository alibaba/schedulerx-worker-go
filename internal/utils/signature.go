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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// HmacSHA1Encrypt encrypt the encryptText use encryptKey
func HmacSHA1Encrypt(encryptText, encryptKey string) string {
	key := []byte(encryptKey)
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(encryptText))
	encryptedStr := hex.EncodeToString(mac.Sum(nil))

	encryptedStr = base64.StdEncoding.EncodeToString([]byte(encryptedStr))
	encryptedStr = strings.ReplaceAll(encryptedStr, "+", "")
	encryptedStr = strings.ReplaceAll(encryptedStr, "=", "")
	encryptedStr = strings.ReplaceAll(encryptedStr, "/", "")

	return encryptedStr
}
