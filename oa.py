import sqlite3
import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import base64
import gspread
import numpy as np
from io import StringIO
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
credentials_json = {
        "type": "service_account",
        "project_id": "noted-handler-379714",
        "private_key_id": "61988f9740276e066ac2fe2aeaac3fa4b6ff527c",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCPWdjJpLudGk7S\ngaV1l/RVUevfJTyrZmKfdMbSkjnpNG4wOGF69rs3h++8luanYcOt+TW87WiAVZum\nUqawNu1GYyh6CyoDxX4lR2/jAZWb/xJ7tgdW7t+rKv1hKTljke8F6kecPSznge/W\npUtegmMFaYTaZJNDuq29A5yT/zo5YWqoxpV0fxpOOlPMvoACelFNcvKsKLnOjaGN\nbjQqFpC+7Sl+LHGaqcO00mTYwbo7Ijj5i+BlYZAwDanN3XSd6Wwl3wnUnGNaWn+6\nVwHyQebXgmICq1iEHNBaoUbEdHDuPXepExoekvSNwx9Ng2vTNCRfzEQVL+3vYSjL\npqo86L9xAgMBAAECggEABQeMl/+N/xQ3F4LptpthHNS52zuL2kIme+x9lNIBUuWu\nZ4X04prxROChuErNlyoSkuIrYOBuAhGu2zorY8ObldSBDS4i2FzHoSB1ZCAwOvfL\nMti3P3U0VwW0O+JVw4DxY1Jd1pUdZBZ+nxfv0eYuefhCq2Rrt8y/D4KGzfj+lph/\nUZvOKQcu9MguBIJAx5xBTtNjeMjhJCdu1OdwyW5MO0irvLmqIQDIphuRvDvXbS93\nSg9zQFxuzLJMKe3jL0zgbkHBjABVnRFycDUWKLgRAmCjoGfY367k800/YNduBqQy\nVBHAQqL9Uuj9S3o5VtZ6uqtjHkhJcDhxnAH7B2G2wQKBgQDGM4fGuMiVTw0rfxSC\npV28y1r5p9tD6uJjTWbqqp0IpX3mjDgZwL0ig5U+3JBBU4n0Ix4WeDjq6/Wy6Tcn\n5Oo9eRwVOsCttl807/sKGBmZV5dRDdbD7NKLBI2sxtbLu1POisK+Si2fJV03mmE6\nm+3Y4DaJ/StovuTuEMIZBKFAoQKBgQC5J4itC5XJ9WCzKIupIF/uxxJGg6TofZvM\nDYkmzb9ZIoh9iFnB5InmlG14ngmRnjINeTqjN4YQtBJNSpk7p1Ra/C2Q+Do/Zm9G\nNWQKd3nYBSQxwNqd97CgzbmtxT8GkSWIvwCtD45oFNkrU0W6eH6/fhGjXNjPj2Ig\nYIHIFNV80QKBgQDALUzUcW0D4M97Qk/n0WHPYjoG4ivnccMq1+0XUnDK5nPp7EGl\nLs30vjMi7Yft34tergJJdS5zEnF8lUbGpt481sZVC0+x36f2003NXsrLdTOiAtIf\nzOvkoXihc3bnue4r0T28dn4/1mHJPSZTRsfbRqN7LoA9owKklpks2uFjoQKBgE0F\n7C6Idjx4jkyZXlfx9tZ/C9Q3qV9p+WjObLKuvp4W5o7KLQSizNcWAeA+Zh6kn4/J\nUaJaU7QZJM/wa4RMXKQo6c+344tCUqHzTfWotBAwO1lTL96tDlYmnspyFoDl2qZj\nRqW3pfcYTStfzc7/l0KT8ER0OGFH9XsginywZgsxAoGAUXm9ge4vbPqKcHNWPYmn\nM/zJ4zkIP57mmRHZBbukLlfXW3/tLHiSZWyotjjDBmcw1yZCoZhTvJcoxeH153V6\nXraQTIW17zjatG22e8E817VKMg/dvHz6KHDHM+DRRxhmGojVsBcUhzPYv/llFoGC\nNYyRe2J3ggufJ0lqCoQNxhg=\n-----END PRIVATE KEY-----\n",
        "client_email": "demo-867@noted-handler-379714.iam.gserviceaccount.com",
        "client_id": "108019621957189237850",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/demo-867%40noted-handler-379714.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, ['https://spreadsheets.google.com/feeds'])
gc = gspread.authorize(credentials)

google_sheet_url = "https://docs.google.com/spreadsheets/d/1FYRwfiVx_NT1w-kR8ZhJ-JPoek1rwmrCw8Em-tlCKz8/edit#gid=0"
sh = gc.open_by_url(google_sheet_url)
worksheet1 = sh.get_worksheet(1)
all_values = worksheet1.get_all_values()
old_data = pd.DataFrame(all_values[1:], columns=all_values[0])
expansecategory = old_data['Expense Category'].unique().tolist()
maintenance_category = old_data['Maintenance Main Category'].unique().tolist()
old_data_columns=old_data.columns[:22]
uploaded_file = st.file_uploader("Choose a file")
expense_categories = ['Select Category', 'All'] + expansecategory



selected_expense = st.sidebar.selectbox('Expense Category', expense_categories, index=0)
update_button = st.sidebar.button('Update')

maintenance_category = ['Select Category', 'All'] + maintenance_category
maintenance_category = st.sidebar.selectbox('Maintenance Category', maintenance_category, index=0)
update_button_maintenance = st.sidebar.button('Update ')


class OA:
    def __init__(self, sh, old_data, selected_expense,old_data_columns):
        self.old_data = old_data
        self.sh = sh
        self.selected_expense = selected_expense
        self.old_data_columns=old_data_columns

    def df_(self):
        if uploaded_file is not None:
            xls = pd.ExcelFile(uploaded_file)
            sheet_names = xls.sheet_names
            year_sheets = [sheet for sheet in sheet_names if 'Year' in pd.read_excel(uploaded_file, sheet_name=sheet).to_string(index=False)]
            sheets = [i for i in year_sheets]
            df = pd.read_excel(uploaded_file, sheet_name=sheets[0], skiprows=1)
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.fillna(0, inplace=True)
            columns = self.old_data_columns.tolist()
            data_with_header = [columns] + df.values.tolist()
            worksheet = sh.get_worksheet(0)
            data = df.values.tolist()
            worksheet.update('A1', data_with_header)
            self.df = df
            st.write("Excel sheet uploaded to Google Sheet successfully!")
            self.merge_df_()

    def merge_df_(self):
        df = self.df
        df.columns=self.old_data_columns
        month_mapping = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
            'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
            'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        df['Month'] = df['Month'].replace(month_mapping)
        self.old_data['Month'] = self.old_data['Month'].replace(month_mapping)
        df['Month'] = df['Month'].astype('int')
        self.old_data['Month'] = self.old_data['Month'].astype('int')
        df['Year'] = df['Year'].astype('int')
        self.old_data['Year'] = self.old_data['Year'].astype('int')
        df['Day'].fillna(1, inplace=True)
        df['Day'] = df['Day'].astype('int')
        self.old_data['Day'] = self.old_data['Day'].astype('int')
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], format='%Y-%m-%d')
        self.old_data['Date'] = pd.to_datetime(self.old_data[['Year', 'Month', 'Day']], format='%Y-%m-%d')
        data_need_to_check = pd.merge(df, self.old_data, how='inner', on=[df.columns[5], df.columns[10]])
        rename_columns = {
            'Date_x': 'old date', 'Date_y': 'New date', 'Area_x': 'Area',
            'Expense-Bearing Branch_x': 'Expense-Bearing Branch', 'Expense Category_x': 'Expense Category',
            'Quantity_x': 'Quantity old', 'Expense Category_y': 'Expense Category new',
            'Quantity_y': 'Quantity new', 'Net Amount_x': 'Net Amount old', 'Net Amount_y': 'Net Amount new'
        }
        data_need_to_check.rename(columns=rename_columns, inplace=True)
        data_need_to_check = data_need_to_check[['old date', 'New date', data_need_to_check.columns[6],
                                                 data_need_to_check.columns[5], 'Area', 'Expense-Bearing Branch',
                                                 'Expense Category', 'Maintenance Main Category', 'Quantity old',
                                                 'Net Amount old', 'Quantity new']]
        self.data_need_to_check = data_need_to_check

    def display_data_selected_expense(self):
        selected_expense = self.selected_expense
        data_need_to_check = self.data_need_to_check
        st.write(selected_expense)
        if selected_expense == 'Select Category':
            pass
        elif selected_expense == 'All':
            for car_number in data_need_to_check[data_need_to_check.columns[3]].unique():
                st.write(car_number)
                data = data_need_to_check[(data_need_to_check[data_need_to_check.columns[3]] == car_number)]
                st.dataframe(data)
        else:
            for car_number in data_need_to_check[data_need_to_check.columns[3]].unique():
                st.write(car_number)
                st.write(selected_expense)
                data = data_need_to_check[(data_need_to_check[data_need_to_check.columns[3]]==car_number) & (data_need_to_check['Expense Category'] == selected_expense)]
                if not data.empty:
                    st.dataframe(data,width=900) 
                else:
                    pass
    def display_data_maintenance_category(self):
        data_need_to_check = self.data_need_to_check
        st.write(maintenance_category)
        if maintenance_category=='Select Category':
            pass
        elif maintenance_category == 'All':
            for car_number in data_need_to_check[data_need_to_check.columns[3]].unique():
                data = data_need_to_check[(data_need_to_check[data_need_to_check.columns[3]] == car_number)]
                st.dataframe(data)
        else:
            for car_number in data_need_to_check[data_need_to_check.columns[3]].unique():
                data = data_need_to_check[(data_need_to_check[data_need_to_check.columns[3]] == car_number) & (data_need_to_check['Maintenance Main Category'] == maintenance_category)]
                if not data.empty:
                    st.dataframe(data)
                else:
                    pass 

if 'selected_expense' not in st.session_state:
    st.session_state.selected_expense = 'Select Category'
if update_button:
    st.session_state.selected_expense = selected_expense

if 'maintenance_category' not in st.session_state:
    st.session_state.maintenance_category='Select Category'
if update_button_maintenance:
    st.session_state.maintenance_category=maintenance_category

obj_oa = OA(sh, old_data,selected_expense,old_data_columns)
if uploaded_file is not None:
    obj_oa.df_() 
if update_button:
    obj_oa.display_data_selected_expense() 
if update_button_maintenance:
    obj_oa.display_data_maintenance_category()
    