/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func checkFileExistsForTest(router *Router, db, table string) bool {
	file := path.Join(router.metadir, db, fmt.Sprintf("%s.json", table))
	if _, err := os.Stat(file); err != nil {
		return false
	}
	return true
}

func makeFileBrokenForTest(router *Router, db, table string) {
	file := path.Join(router.metadir, db, fmt.Sprintf("%s.json", table))
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	fd.Write([]byte("wtf"))
	fd.Close()
}

func TestFrmTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.NotNil(t, err)
	}

	// Add global table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t3", "", TableTypeGlobal, backends, nil)
		assert.Nil(t, err)
	}

	// Add single table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t3_single", "", TableTypeSingle, backends, nil)
		assert.Nil(t, err)
	}

	// Add partition table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t3_partition", "shardkey1", TableTypePartition, backends, nil)
		assert.Nil(t, err)
	}

	// Remove 2.
	{
		tmpRouter := router
		err := router.DropTable("test", "t2")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Refresh table.
	{
		{
			err := router.RefreshTable("test", "t1")
			assert.Nil(t, err)
		}

		{
			err := router.RefreshTable("test", "t2")
			assert.NotNil(t, err)
		}

		{
			err := router.RefreshTable("test", "t3")
			assert.Nil(t, err)
		}
	}
}

func TestFrmTableError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("", "t1", "id", "", backends, nil)
		assert.NotNil(t, err)
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "", "id", "", backends, nil)
		assert.NotNil(t, err)
	}

	// Add single table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "", "", TableTypeSingle, backends, nil)
		assert.NotNil(t, err)
	}

	// Drop table.
	{
		err := router.DropTable("testxx", "t2")
		assert.NotNil(t, err)
	}

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Drop table.
	{
		router.metadir = "/u00000000001/"
		err := router.DropTable("test", "t1")
		assert.NotNil(t, err)
	}
}

func TestFrmDropDatabase(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	{
		tmpRouter := router
		err := router.DropDatabase("test")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}
}

func TestFrmLoad(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	{
		router1, cleanup1 := MockNewRouter(log)
		defer cleanup1()
		assert.NotNil(t, router1)

		// load.
		err := router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)

		// load again.
		err = router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)
	}
}

func TestFrmReadFrmError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	{
		_, err := router.readTableFrmData("/u10000/xx.xx")
		assert.NotNil(t, err)
	}
}

func TestFrmWriteFrmError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	{
		router.metadir = "/u100000/xx"
		err := router.writeTableFrmData("test", "t1", nil)
		assert.NotNil(t, err)
	}
}

func TestFrmReadFileBroken(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
		// Make file broken.
		makeFileBrokenForTest(tmpRouter, "test", "t1")
	}

	// Refresh table.
	{
		{
			err := router.RefreshTable("test", "t1")
			assert.NotNil(t, err)
		}

		{
			err := router.RefreshTable("test", "t2")
			assert.NotNil(t, err)
		}
	}
}

func TestFrmAddTableForTest(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	err := router.AddForTest("test", nil)
	assert.NotNil(t, err)
}

func TestFrmDatabaseNoTables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Tables with database test1.
	router.CreateDatabase("test1")
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test1", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test1", "t1"))
	}

	// Database test2 without tables.
	router.CreateDatabase("test2")

	// Check.
	{
		router1, cleanup1 := MockNewRouter(log)
		defer cleanup1()
		assert.NotNil(t, router1)

		// load.
		err := router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)
	}

	err := router.CreateDatabase("test2")
	assert.NotNil(t, err)
}

func TestFrmTableRename(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Rename 2.
	{
		tmpRouter := router
		err := router.RenameTable("test", "t2", "t3")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t3"))
	}
}

func TestFrmTableRenameError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", "", backends, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Rename t3.
	{
		err := router.RenameTable("test", "t3", "t3")
		assert.NotNil(t, err)
	}

	{
		db := "test"
		fromTable := "t2"
		toTable := "t3"
		dir := path.Join(router.metadir, db)
		file := path.Join(dir, fmt.Sprintf("%s.json", fromTable))
		os.Remove(file)
		err := router.RenameTable(db, fromTable, toTable)
		assert.NotNil(t, err)
	}

	{
		db := "test"
		fromTable := "t1"
		toTable := "t4"
		dir := path.Join(router.metadir, db)
		file := path.Join(dir, fmt.Sprintf("%s.json", toTable))
		_, err := os.Create(file)
		err = os.Chmod(file, 0400)
		err = router.RenameTable("test", fromTable, toTable)
		assert.NotNil(t, err)
		err = os.Chmod(file, 0666)
	}
}
