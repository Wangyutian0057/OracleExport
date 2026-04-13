# OracleExport

本项目用于从 Oracle 数据库导出查询结果到 CSV 或 CSV.GZ 文件，适合大表导出场景，支持日志记录、实时进度显示，以及 `cx_Oracle` / `oracledb` 驱动。

建议在虚拟环境中运行，并使用实际环境参数覆盖连接信息与查询语句。

```powershell
python <export_script>.py `
  --user <oracle_user> `
  --dsn <oracle_dsn> `
  --query "<your_sql>" `
  --out D:\temp\export.csv `
  --log D:\temp\export.log `
  --driver oracledb `
  --thick `
  --oracle-client-lib D:\instantclient
```

README 中已对具体数据库、服务名和业务表名做脱敏处理，请在实际使用时替换为本地配置。

