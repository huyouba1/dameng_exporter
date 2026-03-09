package collector

import (
	"context"
	"dameng_exporter/config"
	"dameng_exporter/logger"
	"dameng_exporter/utils"
	"database/sql"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	unknownAsmGroupName = "UNKNOWN"
	unknownAsmGroupID   = "0"
	unknownAsmGroupType = "UNKNOWN"
)

type asmGroupInfo struct {
	groupID   sql.NullString
	groupName sql.NullString
	groupType sql.NullString
	totalMB   sql.NullFloat64
	freeMB    sql.NullFloat64
	usedPct   sql.NullFloat64
}

// AsmGroupInfoCollector 采集ASM磁盘组总量、剩余量、使用率。
type AsmGroupInfoCollector struct {
	db *sql.DB

	totalDesc   *prometheus.Desc
	freeDesc    *prometheus.Desc
	usedPctDesc *prometheus.Desc
	dataSource  string

	viewCheckOnce sync.Once
	viewExists    bool
}

// SetDataSource 实现 DataSourceAware 接口。
func (c *AsmGroupInfoCollector) SetDataSource(name string) {
	c.dataSource = name
}

func NewAsmGroupInfoCollector(db *sql.DB) MetricCollector {
	return &AsmGroupInfoCollector{
		db: db,
		totalDesc: prometheus.NewDesc(
			dmdbms_asmgroup_size_total_info,
			"ASM disk group total size in MB",
			[]string{"group_name", "group_id", "type"},
			nil,
		),
		freeDesc: prometheus.NewDesc(
			dmdbms_asmgroup_size_free_info,
			"ASM disk group free size in MB",
			[]string{"group_name", "group_id", "type"},
			nil,
		),
		usedPctDesc: prometheus.NewDesc(
			dmdbms_asmgroup_size_used_pct_info,
			"ASM disk group used percentage",
			[]string{"group_name", "group_id", "type"},
			nil,
		),
		viewExists: true,
	}
}

func (c *AsmGroupInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalDesc
	ch <- c.freeDesc
	ch <- c.usedPctDesc
}

func (c *AsmGroupInfoCollector) Collect(ch chan<- prometheus.Metric) {
	if err := utils.CheckDBConnectionWithSource(c.db, c.dataSource); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Global.GetQueryTimeout())*time.Second)
	defer cancel()

	if !c.checkAsmGroupViewExists(ctx) {
		return
	}

	rows, err := c.db.QueryContext(ctx, config.QueryAsmGroupInfoSqlStr)
	if err != nil {
		utils.HandleDbQueryErrorWithSource(err, c.dataSource)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var info asmGroupInfo
		if err := rows.Scan(&info.groupID, &info.groupName, &info.groupType, &info.totalMB, &info.freeMB, &info.usedPct); err != nil {
			logger.Logger.Error("Error scanning V$ASMGROUP row",
				zap.Error(err),
				zap.String("data_source", c.dataSource))
			continue
		}

		groupName := utils.NullStringToString(info.groupName)
		if groupName == "" {
			groupName = unknownAsmGroupName
		}
		groupID := utils.NullStringToString(info.groupID)
		if groupID == "" {
			groupID = unknownAsmGroupID
		}
		groupType := utils.NullStringToString(info.groupType)
		if groupType == "" {
			groupType = unknownAsmGroupType
		}

		ch <- prometheus.MustNewConstMetric(
			c.totalDesc,
			prometheus.GaugeValue,
			utils.NullFloat64ToFloat64(info.totalMB),
			groupName, groupID, groupType,
		)
		ch <- prometheus.MustNewConstMetric(
			c.freeDesc,
			prometheus.GaugeValue,
			utils.NullFloat64ToFloat64(info.freeMB),
			groupName, groupID, groupType,
		)
		ch <- prometheus.MustNewConstMetric(
			c.usedPctDesc,
			prometheus.GaugeValue,
			utils.NullFloat64ToFloat64(info.usedPct),
			groupName, groupID, groupType,
		)
	}

	if err := rows.Err(); err != nil {
		logger.Logger.Warnf("[%s] Iterating V$ASMGROUP rows failed: %v", c.dataSource, err)
	}
}

func (c *AsmGroupInfoCollector) checkAsmGroupViewExists(ctx context.Context) bool {
	c.viewCheckOnce.Do(func() {
		var count int
		if err := c.db.QueryRowContext(ctx, config.QueryAsmGroupViewExistsSqlStr).Scan(&count); err != nil {
			logger.Logger.Warnf("[%s] Failed to check V$ASMGROUP existence: %v", c.dataSource, err)
			c.viewExists = false
			return
		}
		c.viewExists = count > 0
		if !c.viewExists {
			logger.Logger.Infof("[%s] V$ASMGROUP view not found, skip ASM group metrics", c.dataSource)
		}
	})
	return c.viewExists
}
