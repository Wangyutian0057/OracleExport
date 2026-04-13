import argparse
import base64
import csv
import datetime as _dt
import gzip
import logging
import os
import sys
import time
from typing import Any, Iterable, List, Optional, Sequence

#你的数据库配置
#DEFAULT_QUERY = "SQL语句"
#DEFAULT_DSN = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST"IP地址")(PORT="端口")))(CONNECT_DATA=(SERVICE_NAME="表名")))"


def _import_oracle_driver(preferred: str):
    preferred = (preferred or "auto").strip()

    def _load(name: str):
        if name == "cx_Oracle":
            import cx_Oracle  # type: ignore

            return "cx_Oracle", cx_Oracle
        if name == "oracledb":
            import oracledb  # type: ignore

            return "oracledb", oracledb
        raise ValueError(f"unknown driver: {name}")

    if preferred in ("auto", "AUTO"):
        for name in ("cx_Oracle", "oracledb"):
            try:
                return _load(name)
            except Exception:
                continue
        raise RuntimeError("未检测到 Oracle 驱动库。请先安装：pip install cx_Oracle 或 pip install oracledb")

    if preferred in ("cx_Oracle", "oracledb"):
        try:
            return _load(preferred)
        except Exception as e:
            raise RuntimeError(f"加载驱动失败：{preferred}") from e

    raise RuntimeError("driver 参数仅支持：auto / cx_Oracle / oracledb")


def _maybe_enable_oracledb_thick(oracledb_mod, logger: logging.Logger, lib_dir: Optional[str]) -> None:
    is_thin = None
    try:
        if hasattr(oracledb_mod, "is_thin_mode"):
            is_thin = bool(oracledb_mod.is_thin_mode())
    except Exception:
        is_thin = None

    if is_thin is False:
        logger.info("oracledb 已处于 thick 模式")
        return

    try:
        if lib_dir:
            oracledb_mod.init_oracle_client(lib_dir=lib_dir)
        else:
            oracledb_mod.init_oracle_client()
        logger.info("oracledb 已初始化 thick 模式")
    except Exception as e:
        logger.warning("oracledb thick 模式初始化失败（Oracle 11g 可能需要 thick/cx_Oracle）：%s", e)


def _setup_logger(log_path: str, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("oracle_export")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(os.path.dirname(os.path.abspath(log_path)) or ".", exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def _open_output(path: str):
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    if path.lower().endswith(".gz"):
        return gzip.open(path, mode="wt", encoding="utf-8", newline="")
    return open(path, mode="w", encoding="utf-8", newline="")


def _sanitize_headers(headers: List[str]) -> List[str]:
    seen = {}
    out = []
    for h in headers:
        key = h or "COL"
        n = seen.get(key, 0) + 1
        seen[key] = n
        out.append(key if n == 1 else f"{key}_{n}")
    return out


def _to_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float, bool)):
        return str(v)
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        try:
            return v.isoformat(sep=" ")
        except TypeError:
            return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        if len(b) <= 64:
            return b.hex()
        return base64.b64encode(b).decode("ascii")
    if hasattr(v, "read") and callable(getattr(v, "read")):
        try:
            data = v.read()
            return _to_text(data)
        except Exception:
            return str(v)
    return str(v)


def _format_rows(rows: Sequence[Sequence[Any]]) -> Iterable[List[str]]:
    for r in rows:
        yield [_to_text(x) for x in r]


def _estimate_total_rows(conn, query: str, logger: logging.Logger) -> Optional[int]:
    q = query.strip().rstrip(";")
    wrapped = f"select count(1) as CNT from ({q}) Q"
    try:
        cur = conn.cursor()
        try:
            cur.execute(wrapped)
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0])
        finally:
            cur.close()
    except Exception as e:
        logger.warning("总行数统计失败，将按未知总量输出进度：%s", e)
        return None


def _connect(driver_mod, user: str, password: str, dsn: str, encoding: str):
    if not hasattr(driver_mod, "connect"):
        raise RuntimeError("Oracle 驱动不支持 connect()")
    kwargs = {"user": user, "password": password, "dsn": dsn, "encoding": encoding, "nencoding": encoding}
    try:
        return driver_mod.connect(**kwargs)
    except TypeError:
        kwargs.pop("encoding", None)
        kwargs.pop("nencoding", None)
        return driver_mod.connect(**kwargs)


def export_table(
    user: str,
    password: str,
    dsn: str,
    query: str,
    out_path: str,
    log_path: str,
    driver: str,
    thick: bool,
    oracle_client_lib: Optional[str],
    batch_size: int,
    progress_every_rows: int,
    progress_every_seconds: float,
    encoding: str,
    delimiter: str,
    with_total_count: bool,
    verbose: bool,
) -> int:
    logger = _setup_logger(log_path=log_path, verbose=verbose)
    driver_name, driver_mod = _import_oracle_driver(preferred=driver)
    if driver_name == "oracledb" and thick:
        _maybe_enable_oracledb_thick(oracledb_mod=driver_mod, logger=logger, lib_dir=oracle_client_lib)
    logger.info("开始导出：driver=%s out=%s", driver_name, os.path.abspath(out_path))
    logger.info("SQL=%s", query)

    t0 = time.monotonic()
    try:
        conn = _connect(driver_mod=driver_mod, user=user, password=password, dsn=dsn, encoding=encoding)
    except Exception as e:
        msg = str(e)
        if "DPY-3010" in msg:
            logger.error(
                "当前使用的是 oracledb thin 模式，无法连接到该版本数据库（常见于 Oracle 11g）。请改用 thick 模式：--driver oracledb --thick，并配置 Instant Client（--oracle-client-lib）。"
            )
        if "DPI-1047" in msg:
            logger.error(
                "未找到 Oracle Client 库（OCI）。请安装并配置 Oracle Instant Client（把 instantclient 目录加入 PATH），或改用 oracledb thick 模式并指定 --oracle-client-lib。"
            )
        if "ORA-28040" in msg or "authentication protocol" in msg.lower():
            logger.error(
                "连接失败可能由于 Oracle 11g 认证协议不兼容。建议：优先安装 cx_Oracle；或使用 oracledb thick 模式（安装 Instant Client 后加 --thick）。"
            )
        raise

    total_rows = None
    if with_total_count:
        total_rows = _estimate_total_rows(conn, query=query, logger=logger)
        if total_rows is not None:
            logger.info("预计总行数=%d", total_rows)

    rows_done = 0
    last_log_rows = 0
    last_log_t = time.monotonic()

    try:
        cur = conn.cursor()
        cur.arraysize = max(100, batch_size)
        cur.prefetchrows = max(100, batch_size)

        cur.execute(query)
        headers = [d[0] if d and len(d) > 0 else "" for d in (cur.description or [])]
        headers = _sanitize_headers(headers)

        with _open_output(out_path) as f:
            writer = csv.writer(f, delimiter=delimiter, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)

            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                writer.writerows(_format_rows(batch))
                rows_done += len(batch)

                now = time.monotonic()
                need_log_by_rows = (rows_done - last_log_rows) >= progress_every_rows
                need_log_by_time = (now - last_log_t) >= progress_every_seconds
                if need_log_by_rows or need_log_by_time:
                    elapsed = max(0.0001, now - t0)
                    rps = rows_done / elapsed
                    if total_rows is not None and total_rows > 0:
                        pct = min(100.0, rows_done * 100.0 / total_rows)
                        eta = max(0.0, (total_rows - rows_done) / max(0.0001, rps))
                        logger.info(
                            "进度：%d/%d (%.2f%%) 速度：%.0f 行/秒 已用：%.1f 秒 ETA：%.1f 秒",
                            rows_done,
                            total_rows,
                            pct,
                            rps,
                            elapsed,
                            eta,
                        )
                    else:
                        logger.info(
                            "进度：%d 行 速度：%.0f 行/秒 已用：%.1f 秒",
                            rows_done,
                            rps,
                            elapsed,
                        )
                    last_log_rows = rows_done
                    last_log_t = now

        elapsed = max(0.0001, time.monotonic() - t0)
        logger.info("完成：共导出=%d 行 用时=%.1f 秒 速度=%.0f 行/秒", rows_done, elapsed, rows_done / elapsed)
        return 0
    except Exception as e:
        msg = str(e)
        if "ORA-00923" in msg:
            logger.error(
                "SQL 解析失败（ORA-00923）。请尝试用 --query 显式指定更简单的写法，例如：select t.*, rowid from CTN_MOVEMENT_INFO t"
            )
        logger.exception("导出失败：%s", e)
        return 2
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Oracle 大表导出工具（支持日志与实时进度）")
    p.add_argument("--user", default=os.getenv("ORACLE_USER", ""), help="Oracle 用户名（也可用环境变量 ORACLE_USER）")
    p.add_argument(
        "--password",
        default=os.getenv("ORACLE_PASSWORD", ""),
        help="Oracle 密码（也可用环境变量 ORACLE_PASSWORD；不建议在命令行明文输入）",
    )
    p.add_argument("--dsn", default=os.getenv("ORACLE_DSN", DEFAULT_DSN), help="Oracle DSN（也可用环境变量 ORACLE_DSN）")
    p.add_argument("--query", default=DEFAULT_QUERY, help="导出 SQL")
    p.add_argument("--out", required=True, help="输出文件路径（.csv 或 .csv.gz）")
    p.add_argument("--log", default="oracle_export.log", help="日志文件路径")
    p.add_argument(
        "--driver",
        default=os.getenv("ORACLE_DRIVER", "auto"),
        choices=["auto", "cx_Oracle", "oracledb"],
        help="驱动选择。Oracle 11g 推荐 cx_Oracle；oracledb 需要 thick 模式时加 --thick",
    )
    p.add_argument(
        "--thick",
        action="store_true",
        help="仅对 oracledb 生效：启用 thick 模式（通常用于兼容 Oracle 11g，需要安装 Instant Client）",
    )
    p.add_argument(
        "--oracle-client-lib",
        default=os.getenv("ORACLE_CLIENT_LIB", ""),
        help="Instant Client 目录（仅 thick 模式可用），也可用环境变量 ORACLE_CLIENT_LIB",
    )
    p.add_argument("--batch-size", type=int, default=10000, help="每次从数据库抓取行数")
    p.add_argument("--progress-every-rows", type=int, default=200000, help="每导出多少行打印一次进度")
    p.add_argument("--progress-every-seconds", type=float, default=10.0, help="至少每隔多少秒打印一次进度")
    p.add_argument("--encoding", default="utf-8", help="连接与输出编码")
    p.add_argument("--delimiter", default=",", help="CSV 分隔符，默认逗号")
    p.add_argument("--with-total-count", action="store_true", help="先统计总行数（可能较慢，但可显示百分比/ETA）")
    p.add_argument("--verbose", action="store_true", help="控制台输出更多日志")
    args = p.parse_args(argv)
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.user:
        args.user = input("Oracle 用户名: ").strip()
    if not args.password:
        import getpass

        args.password = getpass.getpass("Oracle 密码: ")
    return export_table(
        user=args.user,
        password=args.password,
        dsn=args.dsn,
        query=args.query,
        out_path=args.out,
        log_path=args.log,
        driver=args.driver,
        thick=bool(args.thick),
        oracle_client_lib=(args.oracle_client_lib or None),
        batch_size=args.batch_size,
        progress_every_rows=args.progress_every_rows,
        progress_every_seconds=args.progress_every_seconds,
        encoding=args.encoding,
        delimiter=args.delimiter,
        with_total_count=args.with_total_count,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    raise SystemExit(main())

