import csv
import sys
from datetime import datetime
import psycopg2


# PostgreSQL 연결 정보
DB_CFG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "slamd_metrics",
    "user": "slamd",
    "password": "slamd",
}

# SLAMD UI 시간 포맷
DT_FORMATS = [
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
]


# 문자열 → datetime 변환
def parse_datetime(s: str):
    if not s:
        return None

    s = s.strip()

    for fmt in DT_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    return None


# "1 minute" → 초 단위 변환
def parse_duration_seconds(s: str):

    if not s:
        return None

    s = s.lower()

    total = 0

    if "minute" in s:
        m = int(s.split("minute")[0].strip())
        total += m * 60

    if "second" in s:
        sec = int(s.split("second")[0].split()[-1])
        total += sec

    return total if total else None


# CSV를 섹션 단위로 분리
# ex) [General Information], [Searches Completed] 등
def read_sections(csv_path):

    sections = {}
    current = None

    with open(csv_path, newline="", encoding="utf-8-sig") as f:

        reader = csv.reader(f)

        for row in reader:

            # 빈 줄 skip
            if not row or all(not c.strip() for c in row):
                continue

            first = row[0].strip()

            # 섹션 시작
            if first.startswith("[") and first.endswith("]"):
                current = first.strip("[]")
                sections[current] = []
                continue

            if current:
                sections[current].append([c.strip() for c in row])

    return sections


# header + data → dict 변환
def row_to_map(header, data):
    return {h.strip(): d.strip() for h, d in zip(header, data)}


# 컬럼명이 길어서 suffix로 찾기
# ex) "Searches Completed Count"
def find_value_suffix(m, suffix):

    for k, v in m.items():
        if k.lower().endswith(suffix.lower()):
            return v

    return None


# op_type 우선 매칭 → 없으면 키워드만으로 fallback
# ex) op_type="search", keyword="result codes"
#   1차: "search" in k AND "result codes" in k  → "Search Result Codes" 매칭
#   2차: "result codes" in k                    → "Result Codes" 매칭 (접두사 없는 경우)
def find_section(sections, op_type, keyword):

    # 1차: op_type + keyword 둘 다 포함
    found = next(
        (k for k in sections.keys()
         if op_type in k.lower() and keyword in k.lower()), None
    )
    if found:
        return found

    # 2차: keyword만 포함 (접두사 없는 섹션용 fallback)
    return next(
        (k for k in sections.keys() if keyword in k.lower()), None
    )


# 문자열 → int
def to_int(v):

    if v is None:
        return None

    return int(v.replace(",", ""))


# 문자열 → float
def to_float(v):

    if v is None:
        return None

    return float(v.replace(",", ""))


# slamd_run UPSERT
def upsert_run(cur, run):

    cur.execute(
        """
        INSERT INTO slamd_run (
            job_id, job_type, job_description, job_class, current_state,
            start_time, stop_time, duration_seconds
        )
        VALUES (%(job_id)s,%(job_type)s,%(job_description)s,%(job_class)s,%(current_state)s,
                %(start_time)s,%(stop_time)s,%(duration_seconds)s)
            ON CONFLICT (job_id)
        DO UPDATE SET
            job_type = EXCLUDED.job_type,
                           job_description = EXCLUDED.job_description,
                           job_class = EXCLUDED.job_class,
                           current_state = EXCLUDED.current_state,
                           start_time = EXCLUDED.start_time,
                           stop_time = EXCLUDED.stop_time,
                           duration_seconds = EXCLUDED.duration_seconds
        """,
        run,
    )


# slamd_op_summary UPSERT
def upsert_op_summary(cur, s):

    cur.execute(
        """
        INSERT INTO slamd_op_summary (
            job_id, op_type, total_count, tps, avg_latency_ms, std_dev, corr_coeff, success_rate
        )
        VALUES (%(job_id)s,%(op_type)s,%(total_count)s,%(tps)s,%(avg_latency_ms)s,%(std_dev)s,%(corr_coeff)s,%(success_rate)s)
            ON CONFLICT (job_id, op_type)
        DO UPDATE SET
            total_count = EXCLUDED.total_count,
                           tps = EXCLUDED.tps,
                           avg_latency_ms = EXCLUDED.avg_latency_ms,
                           std_dev = EXCLUDED.std_dev,
                           corr_coeff = EXCLUDED.corr_coeff,
                           success_rate = EXCLUDED.success_rate
        """,
        s,
    )


# slamd_op_bucket UPSERT
def upsert_bucket(cur, b):

    cur.execute(
        """
        INSERT INTO slamd_op_bucket (
            job_id, op_type, bucket_kind, bucket_name, count, percent
        )
        VALUES (%(job_id)s,%(op_type)s,%(bucket_kind)s,%(bucket_name)s,%(count)s,%(percent)s)
            ON CONFLICT (job_id, op_type, bucket_kind, bucket_name)
        DO UPDATE SET
                           count = EXCLUDED.count,
                           percent = EXCLUDED.percent
        """,
        b,
    )


def main():

    if len(sys.argv) != 3:
        print("Usage: python ingest_slamd_csv.py <op_type> <file.csv>")
        sys.exit(1)

    op_type = sys.argv[1].strip().lower()
    csv_path = sys.argv[2]

    sections = read_sections(csv_path)

    # -----------------------
    # General Information
    # -----------------------

    gi = {r[0]: r[1] for r in sections.get("General Information", []) if len(r) >= 2}

    job_id = gi.get("Job ID")

    run = {
        "job_id": job_id,
        "job_type": gi.get("Job Type"),
        "job_description": gi.get("Job Description"),
        "job_class": gi.get("Job Class"),
        "current_state": gi.get("Current State"),
        "start_time": parse_datetime(gi.get("Actual Start Time")),
        "stop_time": parse_datetime(gi.get("Actual Stop Time")),
        "duration_seconds": parse_duration_seconds(gi.get("Actual Duration")),
    }

    # -----------------------
    # Completed section
    # -----------------------

    completed_section = find_section(sections, op_type, "completed")

    total_count = None
    tps = None

    if completed_section:

        rows = sections[completed_section]

        header = rows[0]
        data = rows[1]

        m = row_to_map(header, data)

        total_count = to_int(find_value_suffix(m, "Completed Count"))
        tps = to_float(find_value_suffix(m, "Completed Avg/Second"))

    # -----------------------
    # Duration(ms)
    # -----------------------

    duration_section = find_section(sections, op_type, "duration")

    avg_latency_ms = None
    std_dev = None
    corr = None

    if duration_section:

        rows = sections[duration_section]

        header = rows[0]
        data = rows[1]

        m = row_to_map(header, data)

        avg_latency_ms = to_float(find_value_suffix(m, "Avg Duration"))
        std_dev = to_float(find_value_suffix(m, "Std Dev"))
        corr = to_float(find_value_suffix(m, "Corr Coeff"))

    # -----------------------
    # Result Codes
    # -----------------------

    success_rate = None
    result_buckets = []

    result_codes_section = find_section(sections, op_type, "result codes")
    if result_codes_section:

        rows = sections[result_codes_section]

        for r in rows[1:]:

            name = r[0]
            count = to_int(r[1])
            pct = to_float(r[2])

            result_buckets.append((name, count, pct))

            if name.startswith("0"):
                success_rate = pct / 100.0

    # -----------------------
    # Response Time Categories
    # -----------------------

    response_buckets = []

    response_time_section = find_section(sections, op_type, "response time categories")
    if response_time_section:

        rows = sections[response_time_section]

        for r in rows[1:]:

            name = r[0]
            count = to_int(r[1])
            pct = to_float(r[2])

            response_buckets.append((name, count, pct))

    # -----------------------
    # summary 데이터 생성
    # -----------------------

    op_summary = {
        "job_id": job_id,
        "op_type": op_type,
        "total_count": total_count,
        "tps": tps,
        "avg_latency_ms": avg_latency_ms,
        "std_dev": std_dev,
        "corr_coeff": corr,
        "success_rate": success_rate,
    }

    # -----------------------
    # bucket 생성
    # -----------------------

    buckets = []

    for name, cnt, pct in response_buckets:
        buckets.append(
            {
                "job_id": job_id,
                "op_type": op_type,
                "bucket_kind": "response_time",
                "bucket_name": name,
                "count": cnt,
                "percent": pct / 100.0,
            }
        )

    for name, cnt, pct in result_buckets:
        buckets.append(
            {
                "job_id": job_id,
                "op_type": op_type,
                "bucket_kind": "result_code",
                "bucket_name": name,
                "count": cnt,
                "percent": pct / 100.0,
            }
        )

    # -----------------------
    # DB 저장
    # -----------------------

    conn = psycopg2.connect(**DB_CFG)

    try:

        with conn:

            with conn.cursor() as cur:

                upsert_run(cur, run)

                upsert_op_summary(cur, op_summary)

                for b in buckets:
                    upsert_bucket(cur, b)

        print(f"✅ Ingest 완료: {job_id}")

    finally:

        conn.close()


if __name__ == "__main__":
    main()