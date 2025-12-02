# -*- coding: utf-8 -*-
"""
2000-01-01 ~ 2025-11-30
Top1 Volume/Value 데이터 수집 안정화 버전
- 투자자 데이터, 거래대금 누락, 과거 데이터 구멍 방어 코드 완전 적용
"""

import time
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from pykrx import stock
import FinanceDataReader as fdr


START = "20000101"
END   = "20251130"   # 테스트 연도 먼저

# 속도 & 예의 사이의 타협점 설정
SLEEP_DAILY_TOP = 0.05          # 일별 Top1
SLEEP_OHLCV_TICKER = 0.02       # 종목 OHLCV
SLEEP_INVESTOR_TICKER = 0.05    # 투자자 데이터 (조금 여유)


# ===========================================================
# 1) 거래일 생성 (코스피 지수 기반)
# ===========================================================
def get_trading_days(start, end):
    df = stock.get_index_ohlcv_by_date(start, end, "1001")
    return df.index

# ===========================================================
# 2) 시장 지수 + 시장 투자자 데이터
# ===========================================================
def get_index_data(start: str, end: str) -> pd.DataFrame:
    """코스피/코스닥/나스닥 지수 + 코스피/코스닥 투자자별 매매대금 수집."""

    # 1) KOSPI / KOSDAQ 지수 OHLCV
    kospi = stock.get_index_ohlcv_by_date(start, end, "1001").rename(columns={
        "시가": "kospi_open",
        "고가": "kospi_high",
        "저가": "kospi_low",
        "종가": "kospi_close",
        "거래량": "kospi_volume",
        "거래대금": "kospi_value",
    })
    # 🔹 우리가 쓸 컬럼만 남기기 (상장시가총액 등 제거)
    kospi = kospi[[
        "kospi_open", "kospi_high", "kospi_low",
        "kospi_close", "kospi_volume", "kospi_value"
    ]]

    kosdaq = stock.get_index_ohlcv_by_date(start, end, "2001").rename(columns={
        "시가": "kosdaq_open",
        "고가": "kosdaq_high",
        "저가": "kosdaq_low",
        "종가": "kosdaq_close",
        "거래량": "kosdaq_volume",
        "거래대금": "kosdaq_value",
    })
    kosdaq = kosdaq[[
        "kosdaq_open", "kosdaq_high", "kosdaq_low",
        "kosdaq_close", "kosdaq_volume", "kosdaq_value"
    ]]

    # 2) 코스피 / 코스닥 투자자별 매매대금 (market 단위)
    #   ※ 세 번째 인자로 시장명 ("KOSPI"/"KOSDAQ") 넣기
    kospi_inv = stock.get_market_trading_value_by_date(start, end, "KOSPI")
    kospi_inv = kospi_inv.rename(columns={
        "개인": "kospi_individual_value",
        "외국인": "kospi_foreigner_value",
        "기관합계": "kospi_institution_value",
    })

    # 🔹 과거 구간에는 '외국인'이나 '기관합계'가 없을 수 있으니 NaN 컬럼으로 채워줌
    for col in ["kospi_individual_value", "kospi_foreigner_value", "kospi_institution_value"]:
        if col not in kospi_inv.columns:
            kospi_inv[col] = pd.NA

    kospi_inv = kospi_inv[
        ["kospi_individual_value", "kospi_foreigner_value", "kospi_institution_value"]
    ]

    kosdaq_inv = stock.get_market_trading_value_by_date(start, end, "KOSDAQ")
    kosdaq_inv = kosdaq_inv.rename(columns={
        "개인": "kosdaq_individual_value",
        "외국인": "kosdaq_foreigner_value",
        "기관합계": "kosdaq_institution_value",
    })

    for col in ["kosdaq_individual_value", "kosdaq_foreigner_value", "kosdaq_institution_value"]:
        if col not in kosdaq_inv.columns:
            kosdaq_inv[col] = pd.NA

    kosdaq_inv = kosdaq_inv[
        ["kosdaq_individual_value", "kosdaq_foreigner_value", "kosdaq_institution_value"]
    ]


    # 3) 나스닥 (시차 보정)
    nasdaq = fdr.DataReader("IXIC", start, end)
    nasdaq.index = nasdaq.index + pd.Timedelta(days=1)
    nasdaq = nasdaq.rename(columns={
        "Open": "nasdaq_open",
        "High": "nasdaq_high",
        "Low": "nasdaq_low",
        "Close": "nasdaq_close",
        "Volume": "nasdaq_volume",
    })[[
        "nasdaq_open", "nasdaq_high", "nasdaq_low",
        "nasdaq_close", "nasdaq_volume",
    ]]

    # 4) 전체 merge
    df = kospi.join(kosdaq, how="outer")
    df = df.join(kospi_inv, how="outer")
    df = df.join(kosdaq_inv, how="outer")
    df = df.join(nasdaq, how="outer")

    df.index.name = "date"
    return df

# ===========================================================
# 3) 매일 Top1 Volume/Value 추출
# ===========================================================
def get_top1_by_day(trading_days) -> pd.DataFrame:
    records = []

    for dt in tqdm(trading_days, desc="Collecting daily top1 (volume/value)"):
        date_str = dt.strftime("%Y%m%d")

        try:
            df = stock.get_market_ohlcv_by_ticker(date_str, market="ALL")
        except:
            continue

        # ETF 제외
        etfs = set(stock.get_etf_ticker_list(date_str))
        df = df[~df.index.isin(etfs)]

        if df.empty:
            continue

        # 거래량 1위
        vol_ticker = df["거래량"].idxmax()
        vol_row = df.loc[vol_ticker]

        # 거래대금 1위
        val_ticker = df["거래대금"].idxmax()
        val_row = df.loc[val_ticker]

        # 티커 이름
        try:
            vol_name = stock.get_market_ticker_name(vol_ticker)
        except:
            vol_name = ""
        try:
            val_name = stock.get_market_ticker_name(val_ticker)
        except:
            val_name = ""

        # Volume Top
        records.append({
            "date": dt,
            "rank_type": "VOLUME_TOP",
            "ticker": vol_ticker,
            "name": vol_name,
            "d0_open": vol_row["시가"],
            "d0_high": vol_row["고가"],
            "d0_low": vol_row["저가"],
            "d0_close": vol_row["종가"],
            "d0_volume": vol_row["거래량"],
            "d0_value": vol_row["거래대금"],
        })

        # Value Top
        records.append({
            "date": dt,
            "rank_type": "VALUE_TOP",
            "ticker": val_ticker,
            "name": val_name,
            "d0_open": val_row["시가"],
            "d0_high": val_row["고가"],
            "d0_low": val_row["저가"],
            "d0_close": val_row["종가"],
            "d0_volume": val_row["거래량"],
            "d0_value": val_row["거래대금"],
        })

        time.sleep(SLEEP_DAILY_TOP)

    return pd.DataFrame(records)


# ===========================================================
# 4) OHLCV D-1, D0, D+1 Merge
# ===========================================================
def attach_prev_next_ohlcv(df_top, start, end):
    df_top = df_top.copy()
    df_top["date"] = pd.to_datetime(df_top["date"])
    unique_tickers = df_top["ticker"].unique()

    all_rows = []

    for ticker in tqdm(unique_tickers, desc="Fetching full OHLCV per ticker"):
        try:
            ohlcv = stock.get_market_ohlcv(start, end, ticker)
        except:
            continue

        if ohlcv.empty:
            continue

        # 거래대금 없으면 임시 생성
        if "거래대금" not in ohlcv.columns:
            if "종가" in ohlcv.columns and "거래량" in ohlcv.columns:
                ohlcv["거래대금"] = ohlcv["종가"] * ohlcv["거래량"]
            else:
                continue

        ohlcv = ohlcv.rename(columns={
            "시가":"open","고가":"high","저가":"low",
            "종가":"close","거래량":"volume","거래대금":"value"
        })

        ohlcv["ticker"] = ticker
        ohlcv["date"] = ohlcv.index

        ohlcv = ohlcv.sort_index()
        ohlcv["prev_open"]  = ohlcv["open"].shift(1)
        ohlcv["prev_high"]  = ohlcv["high"].shift(1)
        ohlcv["prev_low"]   = ohlcv["low"].shift(1)
        ohlcv["prev_close"] = ohlcv["close"].shift(1)
        ohlcv["prev_volume"]= ohlcv["volume"].shift(1)
        ohlcv["prev_value"] = ohlcv["value"].shift(1)

        ohlcv["next_open"]  = ohlcv["open"].shift(-1)
        ohlcv["next_high"]  = ohlcv["high"].shift(-1)
        ohlcv["next_low"]   = ohlcv["low"].shift(-1)
        ohlcv["next_close"] = ohlcv["close"].shift(-1)
        ohlcv["next_volume"]= ohlcv["volume"].shift(-1)
        ohlcv["next_value"] = ohlcv["value"].shift(-1)

        all_rows.append(ohlcv)

        time.sleep(SLEEP_OHLCV_TICKER)

    if not all_rows:
        return df_top

    df_ohlcv = pd.concat(all_rows, ignore_index=True)

    # 컬럼 이름 변경
    df_ohlcv = df_ohlcv.rename(columns={
        "open":"d0_open_full","high":"d0_high_full","low":"d0_low_full",
        "close":"d0_close_full","volume":"d0_volume_full","value":"d0_value_full",
        "prev_open":"d-1_open","prev_high":"d-1_high","prev_low":"d-1_low","prev_close":"d-1_close",
        "prev_volume":"d-1_volume","prev_value":"d-1_value",
        "next_open":"d+1_open","next_high":"d+1_high","next_low":"d+1_low","next_close":"d+1_close",
        "next_volume":"d+1_volume","next_value":"d+1_value",
    })

    merged = pd.merge(df_top, df_ohlcv, on=["ticker", "date"], how="left")

    # d0_* 보정
    for col in ["open","high","low","close","volume","value"]:
        d0 = f"d0_{col}"
        full = f"d0_{col}_full"
        if d0 in merged.columns and full in merged.columns:
            merged[d0] = merged[d0].fillna(merged[full])

    return merged


# ===========================================================
# 5) 투자자 데이터 병합 (안정화 버전)
# ===========================================================
def attach_stock_investor_data(df_top, start, end):
    df_top = df_top.copy()
    df_top["date"] = pd.to_datetime(df_top["date"])
    unique_tickers = df_top["ticker"].unique()

    inv_records = []

    for ticker in tqdm(unique_tickers, desc="Fetching investor data per ticker"):
        try:
            inv = stock.get_market_trading_value_by_date(start, end, ticker=ticker)
        except:
            continue

        if inv.empty:
            continue

        inv = inv.rename(columns={
            "개인":"ind_value",
            "외국인":"frg_value",
            "기관합계":"inst_value",
        })

        # 누락 컬럼 보정
        for col in ["ind_value","frg_value","inst_value"]:
            if col not in inv.columns:
                inv[col] = pd.NA

        inv["ticker"] = ticker
        inv["date"] = inv.index

        inv_records.append(inv[["ticker","date","ind_value","frg_value","inst_value"]])

        time.sleep(SLEEP_INVESTOR_TICKER)

    if not inv_records:
        print("[WARN] No investor data collected.")
        return df_top

    df_inv = pd.concat(inv_records, ignore_index=True)

    merged = pd.merge(df_top, df_inv, on=["ticker","date"], how="left")
    return merged


# ===========================================================
# 6) 연도별 파일 저장
# ===========================================================
def main():
    trading_days = get_trading_days(START, END)
    print("Trading days:", len(trading_days))

    df_top = get_top1_by_day(trading_days)
    df_top = attach_prev_next_ohlcv(df_top, START, END)
    df_top = attach_stock_investor_data(df_top, START, END)

    df_index = get_index_data(START, END).reset_index()
    df_index["date"] = pd.to_datetime(df_index["date"])

    df_top["date"] = pd.to_datetime(df_top["date"])
    df_final = pd.merge(df_top, df_index, on="date", how="left")

    df_final["year"] = df_final["date"].dt.year

    for year, df_year in df_final.groupby("year"):
        if year < 2000 or year > 2025:
            continue

        df_year_sorted = df_year.sort_values(["date", "rank_type", "ticker"])

        df_vol = df_year_sorted[df_year_sorted["rank_type"]=="VOLUME_TOP"]
        if not df_vol.empty:
            name = f"top1_volume_{year}.xlsx"
            df_vol.to_excel(name, index=False)
            print("Saved:", name)

        df_val = df_year_sorted[df_year_sorted["rank_type"]=="VALUE_TOP"]
        if not df_val.empty:
            name = f"top1_value_{year}.xlsx"
            df_val.to_excel(name, index=False)
            print("Saved:", name)


if __name__ == "__main__":
    main()
