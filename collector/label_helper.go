package collector

import (
	"dameng_exporter/config"
	"dameng_exporter/db"
)

// buildLabelContextFromConfig 统一构建 datasource 标签与注入器（配置模式）
func buildLabelContextFromConfig(ds *config.DataSourceConfig) (string, *LabelInjector) {
	if ds == nil {
		return "", NewLabelInjectorFromLabels(map[string]string{}, "")
	}

	labels := ds.ParseLabels()
	datasourceLabel := db.BuildDatasourceLabel(ds.Name, ds.DbHost)
	if datasourceLabel == "" {
		datasourceLabel = ds.Name
	}
	labels["datasource"] = datasourceLabel

	return datasourceLabel, NewLabelInjectorFromLabels(labels, ds.Name)
}

// buildLabelContextFromPool 统一构建 datasource 标签与注入器（连接池模式）
func buildLabelContextFromPool(pool *db.DataSourcePool) (string, *LabelInjector) {
	if pool == nil {
		return "", NewLabelInjectorFromLabels(map[string]string{}, "")
	}

	datasourceLabel := pool.Name
	if pool.Labels != nil {
		if dsLabel, ok := pool.Labels["datasource"]; ok && dsLabel != "" {
			datasourceLabel = dsLabel
		}
	}

	return datasourceLabel, NewLabelInjectorFromPool(pool)
}
