import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
import altair as alt

# Initialize connection.
# Uses st.experimental_singleton to only run once.
#@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

#@st.experimental_memo(ttl=600)
def run_query_pandas(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

# Main
# Get the day range. Demonstrates use of "stop" command
option = st.selectbox('Date Range (days):', ('', '7', '14', '28', '90'), index=0)
if (option == ''):
    st.info(f"👆 Please select a timeframe.")
    st.stop()

st.write('Showing data for {} days'.format(option))

conn = init_connection()

sql = '''
select CAST(usage_date AS DATE) AS usage_date, database_name, 
       max(AVERAGE_DATABASE_BYTES+AVERAGE_FAILSAFE_BYTES)/POWER(1024,3) AS TOTAL_BYTES
from   snowflake.account_usage.database_storage_usage_history 
where  usage_date >= dateadd(day, -{}, current_date())
and    usage_date < current_date()
group  by 1,2
order  by 1,2
Limit 5
'''.format(option)

df = run_query_pandas(sql)
st.write(df)

# Stacked Bar chart
c = alt.Chart(df).mark_bar().encode(
    alt.X('USAGE_DATE:T', axis=alt.Axis(format="%Y-%b-%d", labelOverlap=False, labelAngle=-45), bin=True, title="Usage Date"),
    alt.Y('sum(TOTAL_BYTES)', title='Total GB',type='quantitative'),
    color=alt.Color('DATABASE_NAME', type='nominal'),
    order=alt.Order('DATABASE_NAME', sort='ascending')
).properties(title='Total Storage Per Day by Database')

st.altair_chart(c, use_container_width=True)