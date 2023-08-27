/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package bboltstore

import (
	bolt "go.etcd.io/bbolt"
	"os"
	"reflect"
)

func OpenDatabase(dataFile string, dataFilePerm os.FileMode, options ...Option) (*bolt.DB, error) {

	opts := &bolt.Options{}
	for _, opt := range options {
		opt.apply(opts)
	}

	return bolt.Open(dataFile, dataFilePerm, opts)
}

func ObjectType() reflect.Type {
	return BoltStoreClass
}
