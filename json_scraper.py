# -*- coding: utf-8 -*-
"""
CT election scraper → Google Sheets (auto-fallback to versioned URLs)
"""


import os
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


# ---------------- CONFIG ----------------
ELECTION_ID = 97       # 2023-11-07 municipal
SERVICE_ACCOUNT_FILE = "credentials.json"
SHEET_NAME = "municipal-elections-2025-angela"


BASE = "https://ctemsmedia.tgstg.net/ng-app/data/election"
TABS = {
   "Results_EN": "Results_EN",
   "Turnout_EN": "Turnout_EN",
   "Results_ES": "Results_ES",
   "Turnout_ES": "Turnout_ES",
}


PARTY_ES = {
   "Democratic Party": "Partido Demócrata",
   "Republican Party": "Partido Republicano",
   "Working Families Party": "Partido de Familias Trabajadoras",
   "Green Party": "Partido Verde",
   "Independent Party": "Partido Independiente",
   "Write In": "Candidato por escrito",
   "Petitioning Candidate": "Candidato por petición",
   "Conservative Party": "Partido Conservador",
}


# ---------------- HELPERS ----------------
def get_json(url):
   r = requests.get(url, timeout=20)
   r.raise_for_status()
   return r.json()


def fetch_lookup_election(eid):
   """Try plain URLs, then versioned URLs."""
   try:
       # Get version first
       ver = get_json(f"{BASE}/{eid}/Version.json").get("Version")
       try:
           lookup = get_json(f"{BASE}/{eid}/Lookup.json")
           election = get_json(f"{BASE}/{eid}/Election.json")
           return lookup, election, ver
       except requests.HTTPError:
           # Try versioned path
           lookup = get_json(f"{BASE}/{eid}/{ver}/Lookupdata.json")
           election = get_json(f"{BASE}/{eid}/{ver}/Electiondata.json")
           return lookup, election, ver
   except Exception as e:
       raise RuntimeError(f"Could not fetch election {eid}: {e}")


def build_office_map(lst):
   out = {}
   for item in lst:
       if not isinstance(item, dict): continue
       for k, v in item.items():
           out[str(k)] = v.get("NM", "")
   return out


def parse_results(lookup, election):
   towns = lookup.get("townIds", {})
   parties = lookup.get("partyIds", {})
   candidates = lookup.get("candidateIds", {})
   offices = build_office_map(lookup.get("officeList", []))
   town_votes = election.get("townVotes", {})


   rows = []
   for tid, contests in town_votes.items():
       for oid, cands in contests.items():
           for entry in cands:
               for cid, vals in entry.items():
                   cmeta = candidates.get(str(cid), {})
                   pname = parties.get(str(cmeta.get("P", "")), {}).get("NM", "")
                   rows.append({
                       "town_name": towns.get(str(tid), ""),
                       "office_name": offices.get(str(oid), ""),
                       "candidate_name": cmeta.get("NM", ""),
                       "party": pname,
                       "votes": vals.get("V", ""),
                       "percent": vals.get("TO", ""),
                       "town_id": tid, "office_id": oid, "candidate_id": cid,
                   })
   df = pd.DataFrame(rows)
   if not df.empty:
       df["votes"] = pd.to_numeric(df["votes"].astype(str).str.replace(",", ""), errors="coerce")
   return df


def parse_turnout(election):
   vt = election.get("voterTurnout", {})
   town_status = election.get("townStatus", {})
   rows = []
   for tid, d in vt.items():
       # Get precincts_reported from townStatus instead of voterTurnout
       status = town_status.get(str(tid), {})
       precincts_reported = status.get("PR", "")
       
       rows.append({
           "town_id": tid,
           "town_name": d.get("NM", ""),
           "precincts_reported": precincts_reported,
           "electors": d.get("EV", ""),
           "votes_cast": d.get("VV", ""),
           "turnout_percent": d.get("TO", ""),
       })
   df = pd.DataFrame(rows)
   if not df.empty:
       for c in ["electors","votes_cast"]:
           df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
   return df


def to_es_results(df):
   if df.empty: return pd.DataFrame(columns=["Ciudad","Cargo","Candidato","Partido","Votos","Porcentaje"])
   df2 = df.copy()
   df2["party"] = df2["party"].map(lambda x: PARTY_ES.get(x, x))
   return pd.DataFrame({
       "Ciudad": df2["town_name"],
       "Cargo": df2["office_name"],
       "Candidato": df2["candidate_name"],
       "Partido": df2["party"],
       "Votos": df2["votes"],
       "Porcentaje": df2["percent"],
   })


def to_es_turnout(df):
   if df.empty: return pd.DataFrame(columns=["Ciudad","Precintos reportados","Habilitados","Votantes","% Participación"])
   df2 = df.copy()
   df2["Precintos reportados"] = df2["precincts_reported"].astype(str).str.replace(" of "," de ")
   return pd.DataFrame({
       "Ciudad": df2["town_name"],
       "Precintos reportados": df2["Precintos reportados"],
       "Habilitados": df2["electors"],
       "Votantes": df2["votes_cast"],
       "% Participación": df2["turnout_percent"],
   })


def ensure_ws(sh, name):
   try:
       ws = sh.worksheet(name)
       ws.clear()
       return ws
   except gspread.exceptions.WorksheetNotFound:
       return sh.add_worksheet(name, rows=2, cols=2)


def write_df(sh, name, df):
   ws = ensure_ws(sh, name)
   set_with_dataframe(ws, df)


# ---------------- MAIN ----------------
def main():
   lookup, election, ver = fetch_lookup_election(ELECTION_ID)
   df_res = parse_results(lookup, election)
   df_turn = parse_turnout(election)
   df_res_es = to_es_results(df_res)
   df_turn_es = to_es_turnout(df_turn)


   meta = {
       "election_id": ELECTION_ID,
       "version": ver,
       "scraped_at": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
   }
   for df in (df_res, df_turn, df_res_es, df_turn_es):
       for k,v in meta.items(): df[k] = v


   creds = Credentials.from_service_account_file(
       SERVICE_ACCOUNT_FILE,
       scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"],
   )
   gc = gspread.authorize(creds)
   try:
       sh = gc.open(SHEET_NAME)
   except gspread.SpreadsheetNotFound:
       sh = gc.create(SHEET_NAME)


   write_df(sh, TABS["Results_EN"], df_res)
   write_df(sh, TABS["Turnout_EN"], df_turn)
   write_df(sh, TABS["Results_ES"], df_res_es)
   write_df(sh, TABS["Turnout_ES"], df_turn_es)


   print(f"✅ Election {ELECTION_ID}, version {ver}")
   print(f"Results_EN {len(df_res):,} rows  Turnout_EN {len(df_turn):,} rows")
   print(f"Wrote to {SHEET_NAME}")


if __name__ == "__main__":
   main()