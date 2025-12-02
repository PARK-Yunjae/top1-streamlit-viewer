# app.py
# 거래량/거래대금 1위 데이터 뷰어 + 간단 종가→익일 시가 백테스트

import streamlit as st
import pandas as pd
from pathlib import Path

# -----------------------
# 기본 설정
# -----------------------
st.set_page_config(
    page_title="거래량/거래대금 1위 데이터 앱",
    layout="wide",
)
    
st.title("📊 거래량/거래대금 1위 데이터 뷰어 & 종가→익일 시가 전략 테스트")

# 엑셀 파일들이 있는 폴더 (app.py와 같은 위치라고 가정)
DATA_DIR = Path(".")

# 연도 범위 (필요하면 조정)
START_YEAR = 2000
END_YEAR = 2025

COLUMN_KOR = {
    "date": "날짜",
    "rank_type": "순위구분",
    "ticker": "종목코드",
    "name": "종목명",
    "d0_open": "당일 시가",
    "d0_high": "당일 고가",
    "d0_low": "당일 저가",
    "d0_close": "당일 종가",
    "d0_volume": "당일 거래량",
    "d0_value": "당일 거래대금",
    "d+1_open": "익일 시가",
    "d+1_close": "익일 종가",
    "kospi_close": "코스피 종가",
    "kosdaq_close": "코스닥 종가",
    "nasdaq_close": "나스닥 종가",
}


# -----------------------
# 데이터 로딩 함수
# -----------------------
@st.cache_data
def load_top1_data(kind: str) -> pd.DataFrame:
    """
    kind: 'volume' 또는 'value'
    - top1_volume_YYYY.xlsx
    - top1_value_YYYY.xlsx
    파일들을 모두 읽어서 하나의 DataFrame으로 합침.
    """
    dfs = []
    prefix = "top1_volume" if kind == "volume" else "top1_value"

    for year in range(START_YEAR, END_YEAR + 1):
        fname = DATA_DIR / f"{prefix}_{year}.xlsx"
        if not fname.exists():
            continue

        df = pd.read_excel(fname)

        # 날짜 컬럼을 datetime으로 통일
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        else:
            # date 컬럼이 없으면 이 파일은 건너뜀
            continue

        # 연도 컬럼이 없다면 생성
        if "year" not in df.columns:
            df["year"] = df["date"].dt.year

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    all_df = pd.concat(dfs, ignore_index=True)
    all_df = all_df.sort_values("date").reset_index(drop=True)
    return all_df


def get_date_bounds(df: pd.DataFrame):
    if df.empty:
        return None, None
    return df["date"].min(), df["date"].max()


# -----------------------
# 전략 백테스트 함수
# -----------------------
def backtest_next_open(
    df: pd.DataFrame,
    upper_pct: float,
    lower_pct: float,
    fee_pct: float,
    capital: int = 1_000_000,
):
    """
    당일 종가(d0_close) 매수 -> 익일 시가(d+1_open) 매도 전략

    - upper_pct : 상단 이익 제한 (%)  예: 3.0  -> +3% 이상은 +3%로 잘라서 계산
    - lower_pct : 하단 손절 (%)      예: -1.0 -> -1% 이하는 -1%로 잘라서 계산
    - fee_pct   : 왕복 수수료/세금/슬리피지 (%) 예: 0.5
    """
    required_cols = {"d0_close", "d+1_open", "date"}
    if not required_cols.issubset(df.columns):
        return None, f"필요 컬럼 부족: {required_cols - set(df.columns)}"

    work = df.dropna(subset=["d0_close", "d+1_open", "date"]).copy()
    if work.empty:
        return None, "유효한 데이터가 없습니다 (d0_close 또는 d+1_open NaN)"

    # 원시 수익률: 익일 시가 / 당일 종가 - 1
    work["raw_ret"] = work["d+1_open"] / work["d0_close"] - 1.0

    # 상하한 제한 (클리핑)
    up = upper_pct / 100.0
    down = lower_pct / 100.0
    work["clipped_ret"] = work["raw_ret"].clip(lower=down, upper=up)

    # 수수료/세금 반영 (왕복 fee_pct%)
    fee = fee_pct / 100.0
    work["net_ret"] = (1 + work["clipped_ret"]) * (1 - fee) - 1

    # 누적 자본曲선
    work = work.sort_values("date")
    work["equity"] = capital * (1 + work["net_ret"]).cumprod()

    # 통계 계산
    n_trades = len(work)
    wins = (work["net_ret"] > 0).sum()
    win_rate = (wins / n_trades * 100) if n_trades > 0 else 0.0
    avg_ret = work["net_ret"].mean() * 100 if n_trades > 0 else 0.0
    total_ret = (work["equity"].iloc[-1] / capital - 1) * 100 if n_trades > 0 else 0.0

    stats = {
        "n_trades": n_trades,
        "wins": wins,
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "total_ret": total_ret,
        "final_equity": work["equity"].iloc[-1] if n_trades > 0 else capital,
        "equity_series": work[["date", "equity"]],
    }
    return stats, None


# -----------------------
# 사이드바 UI
# -----------------------
st.sidebar.header("⚙️ 설정")

data_kind_label = st.sidebar.selectbox(
    "데이터 종류 선택",
    ["거래량 1위", "거래대금 1위"],
)
data_kind = "volume" if data_kind_label == "거래량 1위" else "value"

df_all = load_top1_data(data_kind)

if df_all.empty:
    st.error("엑셀 데이터를 찾을 수 없습니다.\n같은 폴더에 top1_volume_YYYY.xlsx / top1_value_YYYY.xlsx 가 있는지 확인해주세요.")
    st.stop()

min_date, max_date = get_date_bounds(df_all)
if min_date is None or max_date is None:
    st.error("날짜 정보가 없습니다.")
    st.stop()

# 날짜 범위 선택
st.sidebar.subheader("📅 날짜 범위")
date_range = st.sidebar.date_input(
    "조회 기간",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# 금요일 제외
exclude_friday = st.sidebar.checkbox("금요일(weekday=4) 제외", value=False)

st.sidebar.caption("※ ETF는 수집 과정에서 이미 제외된 상태라고 가정합니다.")


# -----------------------
# 메인: 필터링
# -----------------------
filt = (df_all["date"] >= pd.to_datetime(start_date)) & (df_all["date"] <= pd.to_datetime(end_date))
df_view = df_all[filt].copy()

if exclude_friday:
    df_view = df_view[df_view["date"].dt.weekday != 4]

st.write(f"### 🔎 현재 선택된 구간: {len(df_view):,} 행 (데이터 종류: {data_kind_label})")

# 기본으로 보여줄 컬럼 후보
# ----- 표로 보고 싶은 컬럼(목차) 선택 -----
st.subheader("📄 표로 보고 싶은 컬럼(목차) 선택")

# 👉 화면에 보여줄 때만 한글로 컬럼명 변경
df_table = df_view.rename(columns=COLUMN_KOR)

all_columns = list(df_table.columns)

# 영문 기준 기본 컬럼 9개
default_en = [
    "date",
    "rank_type",
    "ticker",
    "name",
    "d0_close",
    "d+1_open",
    "kospi_close",
    "kosdaq_close",
    "nasdaq_close",
]
# 그걸 한글 이름으로 변환
default_candidates = [COLUMN_KOR.get(c, c) for c in default_en]
default_cols = [c for c in default_candidates if c in all_columns]

selected_cols = st.multiselect(
    "표에 표시할 컬럼들 선택",
    options=all_columns,
    default=default_cols if default_cols else all_columns,
)

if selected_cols:
    # ✅ 표는 한글 컬럼명으로 보여줌
    st.dataframe(
        df_table[selected_cols].sort_values("날짜"),
        use_container_width=True,
    )
else:
    st.info("표로 보고 싶은 컬럼을 하나 이상 선택해주세요.")

# -----------------------
# 전략 백테스트 영역
# -----------------------
st.subheader("📈 종가 → 익일 시가 전략 수익률 계산")

col1, col2, col3, col4 = st.columns(4)
with col1:
    upper_input = st.number_input("상단 이익 제한(%)", value=3.0, step=0.5)
with col2:
    lower_input = st.number_input("하단 손절(%)", value=-1.0, step=0.5)
with col3:
    fee_pct = st.number_input("수수료+세금(왕복, %)", value=0.5, step=0.1)
with col4:
    capital = st.number_input("초기 자본(원)", value=1_000_000, step=100_000)

# 프리셋 선택
preset = st.selectbox(
    "전략 프리셋 (원하면 선택, 아니면 '직접 입력' 유지)",
    ["직접 입력", "+3 / -1", "+5 / -1", "+2 / -1", "+3 / -2", "+10 / -1"],
)

# 실제 사용할 upper/lower 결정
upper_pct, lower_pct = upper_input, lower_input
if preset != "직접 입력":
    if preset == "+3 / -1":
        upper_pct, lower_pct = 3.0, -1.0
    elif preset == "+5 / -1":
        upper_pct, lower_pct = 5.0, -1.0
    elif preset == "+2 / -1":
        upper_pct, lower_pct = 2.0, -1.0
    elif preset == "+3 / -2":
        upper_pct, lower_pct = 3.0, -2.0
    elif preset == "+10 / -1":
        upper_pct, lower_pct = 10.0, -1.0

st.caption(f"※ 현재 적용될 상단/하단: {upper_pct:.2f}% / {lower_pct:.2f}%")

if st.button("🚀 이 설정으로 전략 수익률 계산하기"):
    stats, err = backtest_next_open(
        df_view,
        upper_pct=upper_pct,
        lower_pct=lower_pct,
        fee_pct=fee_pct,
        capital=int(capital),
    )

    if err is not None:
        st.error(err)
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("총 거래 횟수", f"{stats['n_trades']:,} 회")
        with c2:
            st.metric("승률", f"{stats['win_rate']:.2f} %")
        with c3:
            st.metric("평균 수익률(회당)", f"{stats['avg_ret']:.3f} %")
        with c4:
            st.metric("누적 수익률", f"{stats['total_ret']:.2f} %")

        st.markdown(f"**최종 자본**: {stats['final_equity']:,.0f} 원")

        # 에쿼티 커브
        eq_df = stats["equity_series"].set_index("date")
        st.line_chart(eq_df, height=260)


# -----------------------
# CSV 다운로드
# -----------------------
st.subheader("💾 현재 필터 결과 데이터 다운로드")

if not df_view.empty:
    csv_bytes = df_view.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "현재 조건의 데이터를 CSV로 다운로드",
        data=csv_bytes,
        file_name="filtered_top1_data.csv",
        mime="text/csv",
    )
else:
    st.info("현재 조건에 해당하는 데이터가 없습니다.")
