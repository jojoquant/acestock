from jotdx.quotes import Quotes
from jotdx.consts import MARKET_SH, MARKET_SZ

client = Quotes.factory(market='std')

sh_df = client.stocks(market=MARKET_SH)
sh_stock_df = sh_df[sh_df['code'].str.contains("^((688)[\d]{3}|(60[\d]{4}))$")]
sh_bond_df = sh_df[sh_df['code'].str.contains("^(110|113)[\d]{3}$")]
sh_etf_df = sh_df[sh_df['code'].str.contains("^(58|51|56)[\d]{4}$")]
print(1)

sz_df = client.stocks(market=MARKET_SZ)
sz_stock_df = sz_df[sz_df['code'].str.contains("^((002|000|300)[\d]{3})$")]
sz_bond_df = sz_df[sz_df['code'].str.contains("^((127|128|123)[\d]{3})$")]
sz_etf_df = sz_df[sz_df['code'].str.contains("^(15)[\d]{4}$")]
print(1)