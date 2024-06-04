// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"fmt"
	"os"
	"testing"

	"database/sql"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	_ "github.com/go-sql-driver/mysql"
)

var loadedLocalLocation *time.Location

func LoadedLocalLocation() *time.Location {
	var err error
	loadedLocalLocation, err = time.LoadLocation(time.Local.String())
	if err != nil {
		panic(err)
	}
	if loadedLocalLocation == nil {
		panic("nil LoadedLocalLocation " + time.Local.String())
	}
	return loadedLocalLocation
}

type Item struct {
	ItemId            uint       `gorm:"primaryKey;autoIncrement" json:"itemId"`
	ItemNumber        string     `gorm:"not null;size:20;uniqueIndex:idx_item" json:"itemNumber"`
	XdotType          string     `gorm:"not null;size:10;uniqueIndex:idx_item" json:"xdotType"`
	CreateAt          time.Time  `gorm:"autoCreateTime:RFC3339;type:DATETIME" json:"createAt"`
	UpdateAt          time.Time  `gorm:"autoUpdateTime:RFC3339;type:DATETIME" json:"updateAt"`
	CreatedBy         *uint      `gorm:"" json:"createdBy"`
	CreatedByInfo     User       `gorm:"foreignKey:CreatedBy;references:UserId;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;" json:"createdByInfo"`
	TestFirmware      *string    `gorm:"size:10" json:"testFirmware"`
	TestFirmwareInfo  XDFirmware `gorm:"foreignKey:TestFirmware;references:FWId;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT;" json:"testFirmwareInfo"`
	WriteFirmware     *string    `gorm:"size:10" json:"writeFirmware"`
	WriteFirmwareInfo XDFirmware `gorm:"foreignKey:WriteFirmware;references:FWId;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT;" json:"writeFirmwareInfo"`
}

type User struct {
	UserId        uint    `gorm:"primaryKey;autoIncrement" json:"userId"`
	UserFirstName string  `gorm:"not null" json:"userFirstName"`
	UserLastName  string  `gorm:"not null" json:"userLastName"`
	UserUsername  string  `gorm:"not null;unique;<-:create" json:"userUsername"`
	UserPassword  string  `gorm:"not null" json:"userPassword"`
	UserAvatar    *[]byte `gorm:"size:2097152" json:"userAvatar"`
	AuthLevel     uint    `gorm:"-:all" json:"authLevel,omitempty"`
}

type XDFirmware struct {
	FWId        string  `gorm:"primaryKey;not null;unique;size:10" json:"fwId"`
	Version     string  `gorm:"not null;size:50;uniqueIndex:idx_fw_vers" json:"version"`
	MbedVersion string  `gorm:"not null;size:50;uniqueIndex:idx_fw_vers" json:"mbedVersion"` // Version string when do `ati` command
	Freq        string  `gorm:"not null;size:10;uniqueIndex:idx_fw_vers" json:"freq"`
	TargetXdot  string  `gorm:"not null;size:10;uniqueIndex:idx_fw_vers" json:"targetXdot"` // Based on `controller.XDType`
	Checksum    string  `gorm:"not null;size:32;unique" json:"checksum"`
	Path        *string `gorm:"check:COALESCE(path, file_content) IS NOT NULL" json:"path"` // Path where the firmware located
	File        struct {
		FileContent *[]byte `gorm:"check:COALESCE(path, file_content) IS NOT NULL" json:"fileContent"`                                  // Uploaded file. The firmware stored as bytes
		FileName    *string `gorm:"check:file_content IS NULL OR (file_content IS NOT NULL AND file_name IS NOT NULL)" json:"fileName"` // The name of the uploaded file
		MimeType    *string `gorm:"check:file_content IS NULL OR (file_content IS NOT NULL AND mime_type IS NOT NULL)" json:"mimeType"` // Mime type for the uploaded file
	} `gorm:"embedded" json:"file"`
	CreateAt         time.Time `gorm:"autoCreateTime;type:DATETIME" json:"createAt"`
	UpdateAt         time.Time `gorm:"autoUpdateTime;type:DATETIME" json:"updateAt"`
	CreatedBy        *uint     `gorm:"" json:"createdBy"`
	CreatedByInfo    User      `gorm:"foreignKey:CreatedBy;references:UserId;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;" json:"createdByInfo"`
	PathFileChecksum string    `gorm:"-:all" json:"pathFileChecksum,omitempty"`
	UpFileChecksum   string    `gorm:"-:all" json:"upFileChecksum,omitempty"`
}

func TestGorm(t *testing.T) {
	// Dolt and MySQL truncate to microseconds
	createTime1 := time.Now().UTC().Truncate(time.Microsecond)
	updateTime1 := createTime1.Add(time.Hour)

	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(dir)
	}()

	var AllModels = []any{&User{}, &XDFirmware{}, &Item{}}

	// Connect to the server database
	dbName := "server_db"
	dsn := fmt.Sprintf("file://%v?commitname=%v&commitemail=%v&database=%v", dir, "Gorm Tester", "gorm@dolthub.com", dbName)
	sqlDB, err := sql.Open("dolt", dsn)
	require.NoError(t, err)

	// Init Dolt database if not exist
	_, err = sqlDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	require.NoError(t, err)
	defer sqlDB.Close()

	// Connect Dolt database to GORM
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: sqlDB}), &gorm.Config{SkipDefaultTransaction: true, PrepareStmt: true})
	defer sqlDB.Close()
	require.NoError(t, err)

	// Now run migrations
	err = db.AutoMigrate(AllModels...)
	require.NoError(t, err)

	// Insert some objects
	bytes := []byte("user.jpg")
	str := "a string"
	user := User{
		UserId:        1,
		UserFirstName: "John",
		UserLastName:  "Doe",
		UserUsername:  "johndoe",
		UserPassword:  "pass",
		UserAvatar:    &bytes,
		AuthLevel:     2,
	}
	tx := db.Save(&user)
	require.NoError(t, tx.Error)

	firmware1 := XDFirmware{
		FWId:        "fw1",
		Version:     "1.0.0",
		MbedVersion: "1.0.0",
		Freq:        "868",
		TargetXdot:  "XDOT",
		Checksum:    "1234567890",
		Path:        nil,
		File: struct {
			FileContent *[]byte `gorm:"check:COALESCE(path, file_content) IS NOT NULL" json:"fileContent"`
			FileName    *string `gorm:"check:file_content IS NULL OR (file_content IS NOT NULL AND file_name IS NOT NULL)" json:"fileName"`
			MimeType    *string `gorm:"check:file_content IS NULL OR (file_content IS NOT NULL AND mime_type IS NOT NULL)" json:"mimeType"`
		}{
			FileContent: &bytes,
			FileName:    &str,
			MimeType:    &str,
		},
		CreateAt:         createTime1,
		UpdateAt:         updateTime1,
		CreatedBy:        &user.UserId,
		CreatedByInfo:    user,
		PathFileChecksum: "",
		UpFileChecksum:   "",
	}

	tx = db.Save(&firmware1)
	require.NoError(t, tx.Error)

	item := Item{
		ItemId:            1,
		ItemNumber:        "item1",
		XdotType:          "XDOT",
		CreateAt:          createTime1,
		UpdateAt:          updateTime1,
		CreatedBy:         &user.UserId,
		CreatedByInfo:     user,
		TestFirmware:      &firmware1.FWId,
		TestFirmwareInfo:  firmware1,
		WriteFirmware:     &firmware1.FWId,
		WriteFirmwareInfo: firmware1,
	}

	tx = db.Save(&item)
	require.NoError(t, tx.Error)

	// Query the objects
	var findItem Item
	// TODO: filtering on created date as inserted doesn't work, not sure if it's because the database is inserting 
	//  its own timestamp or some kind of timezone issue 
	result := db.
		Preload("CreatedByInfo").
		Preload("TestFirmwareInfo").
		Preload("TestFirmwareInfo.CreatedByInfo").
		Preload("WriteFirmwareInfo").
		Preload("WriteFirmwareInfo.CreatedByInfo").
		First(&findItem)	
	require.NoError(t, result.Error)
	assert.Equal(t, scrubItems(item, createTime1, updateTime1), scrubItems(findItem, createTime1, updateTime1))
	
	// Re-running migration should work fine
	err = db.AutoMigrate(AllModels...)
	require.NoError(t, err)

	findItem = Item{}
	result = db.
		Preload("CreatedByInfo").
		Preload("TestFirmwareInfo").
		Preload("TestFirmwareInfo.CreatedByInfo").
		Preload("WriteFirmwareInfo").
		Preload("WriteFirmwareInfo.CreatedByInfo").
		First(&findItem)
	require.NoError(t, result.Error)
	assert.Equal(t, scrubItems(item, createTime1, updateTime1), scrubItems(findItem, createTime1, updateTime1))
}

// scrubItems returns an item with the create and update times set to the given values for comparison purposes since 
// the database connection returns time objects with different timezone information internals that can't be compared 
// with testify
func scrubItems(item Item, create time.Time, update time.Time) Item {
	item.CreateAt	= create
	item.UpdateAt = update
	item.CreatedByInfo.AuthLevel = 0 // this field is also auto assigned by the DB
	item.TestFirmwareInfo.CreateAt = create
	item.TestFirmwareInfo.UpdateAt = update
	item.TestFirmwareInfo.CreatedByInfo.AuthLevel = 0
	item.WriteFirmwareInfo.CreateAt = create
	item.WriteFirmwareInfo.UpdateAt = update
	item.WriteFirmwareInfo.CreatedByInfo.AuthLevel = 0
	return item
}

