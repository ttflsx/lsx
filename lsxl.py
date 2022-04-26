# from typing_extensions import Concatenate
import numpy as np
from logging import error
from mimetypes import MimeTypes
from pandas.io.pytables import Table
import streamlit as st
import datetime
import datetime as dt # to work with date, time
from bs4 import BeautifulSoup # to work with web scrapping (HTML)
import pandas as pd # to work with tables (DataFrames) data
from IPython.core.display import HTML
from streamlit.elements import multiselect # to display HTML in the notebook
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
import gspread #-> Để update data lên Google Spreadsheet
from gspread_dataframe import set_with_dataframe #-> Để update data lên Google Spreadsheet
from oauth2client.service_account import ServiceAccountCredentials #-> Để nhập Google Spreadsheet Credentials
import gspread_dataframe as gd
import gspread as gs
from gspread.utils import A1_ADDR_ROW_COL_RE
st.set_page_config(layout='wide')
def pull_lsx(gc):
    spreadsheet_key='1dUUWEBwnD4kSJAwI3Oi4_fXN1Yji8cdth4Rs2RewCuw'
    sh=gc.open('SX1.1 - Database ĐHNB 2022').worksheet('1.Master DH')
    sheet=sh.get_all_values()
    ncc=pd.DataFrame(sheet)
    ncc=ncc.astype(str)
    ncc.columns=ncc.iloc[0]
    ncc=ncc[1:]
    ncc["SỐ ĐƠN HÀNG"]=ncc["SỐ ĐH"]

    sh2=gc.open('LSX - lưu trữ').worksheet('LSX ĐÃ IN')
    sheet2=sh2.get_all_values()
    lsx_cu=pd.DataFrame(sheet2)
    lsx_cu=lsx_cu.astype(str)
    lsx_cu.columns=lsx_cu.iloc[0]
    list=lsx_cu['LỆNH SX'].unique().tolist()
    # list
    ncc=ncc[ncc["LỆNH SX"].isin(list)==False]
    return ncc,lsx_cu
# ncc_list=ncc()

def push_lsx(df,ws1,ws2):
    data_list = df.values.tolist()
    ws1.append_rows(data_list)
    ws2.append_rows(data_list)
    st.success('Done')


    st.success('Done')
def download_link(object_to_download, download_filename, download_link_text):
    import base64,io
    if isinstance(object_to_download,pd.DataFrame):
        # object_to_download = object_to_download.to_excel(index = False, header=True,encoding="cp1258")
            
        towrite = io.BytesIO()
        downloaded_file = object_to_download.to_excel(towrite, encoding='utf-8', index=False, header=True) # write to BytesIO buffer
        towrite.seek(0)  # reset pointer
        b64 = base64.b64encode(towrite.read()).decode() 
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="myfilename.xlsx">Bấm vào đây để tải danh sách về</a>'

def push_lsx_ver2(df,ws1,ws2):

    import gspread_dataframe as gd
    import gspread as gs

    existing1 = gd.get_as_dataframe(ws1)
    existing1=existing1[existing1['LỆNH SX'].isnull()==False]
    updated1 = existing1.append(df)
    gd.set_with_dataframe(ws1,updated1)

    existing2 = gd.get_as_dataframe(ws2)
#     existing2=existing2.astype(str)

    col = 'LỆNH SX'
    cols_to_replace = ["TÊN SẢN PHẨM TTF",'SỐ LƯỢNG', 'LOẠI GỖ','MÀU SƠN','Versionn']
    # updated2=existing2.loc[existing2[col].isin(df[col]), cols_to_replace] = df.loc[df[col].isin(existing2[col]),cols_to_replace].values
    existing2.loc[existing2[col].isin(df[col])==True, cols_to_replace]=df[cols_to_replace].values
    df      # 
    gd.set_with_dataframe(ws2, existing2)

    st.success('Done')


credentials = service_account.Credentials.from_service_account_info(
st.secrets["gcp_service_account"],
scopes=['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'],
)
gc = gspread.authorize(credentials)
st.title('DANH SÁCH LỆNH SẢN XUẤT')
colu1,colu2,cll3=st.columns(3)
df_df=pull_lsx(gc)
df1=df_df[0].astype(str)
lsx_cu=df_df[1]
with colu1:
    username = st.text_input("User Name")
    aa=st.checkbox("Login")

with colu2:
    password = st.text_input("Password",type='password')
if aa:
    if  password==st.secrets["passwords"] and username==st.secrets['user']:
        # c0,c1,c2,c3,c4,c5,c6,c7= st.columns((1.8,1,.9,.9,.9,.9,.9,9))
        
        if 'count' not in st.session_state:
            rows = 0
        select=st.selectbox('Chọn',['RA LSX MỚI','CẬP NHẬT LẠI LSX CŨ'])
        if select=='RA LSX MỚI':

            dhsx=df1["SỐ ĐƠN HÀNG"].unique().tolist()

            list_sdh=st.multiselect("Nhập số đơn hàng",dhsx)
            df=df1[df1["SỐ ĐƠN HÀNG"].isin(list_sdh)]
            with st.form(key='columns_in_form'):
                c0,c1,c2,c3,c4,c5,c6,c7,c8= st.columns((1.8,2,3,1,.9,.9,.9,.9,.9))

                list_r=df["LỆNH SX"].tolist()
                kh_r=df["TÊN KHÁCH HÀNG"].tolist()  
                sp=df["TÊN SẢN PHẨM TTF"].tolist()
                # cols = st.beta_columns(5)
                # for i, col in enumerate(cols):
                rows = len(list_r)


                # list_r=[50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200]
                with c0:
                    lsx=[]
                    for nr in range(rows):
                        lsx.append(c0.selectbox('',[list_r[nr]], key=f'dfuestidn {nr}'))
                    # st.selectbox('Lệnh sản xuất',['a','b','c'])
                with c3:

                    nm=[]
                    for nr in range(rows):
                        nm.append(c3.selectbox('Nhà máy',["",'NM1','NM3','NM5'], key=f'dfquestidn {nr}'))
                    # st.selectbox('Lệnh sản xuất',['a','b','c'])
                with c4:
                    ldh=[]
                    for nr in range(rows):
                        ldh.append(c4.selectbox('Loại đơn hàng',["",'C',"M"], key=f'dfquesatdidn {nr}'))  
                with c5:
                    gc1=[]
                    for nr in range(rows):
                        gc1.append(c5.selectbox('Gia công ',["",'N',"Y"], key=f'dfqudesưtdidn {nr}')) 
                with c6:
                    uc=[]
                    for nr in range(rows):
                        uc.append(c6.selectbox('V/e uốn cong ',["",'N',"Y"], key=f'dfqudesưtdidn{nr}')) 
                with c7:
                    vn=[]
                    for nr in range(rows):
                        vn.append(c7.selectbox('Verneer ',["",'N',"Y"], key=f'dfqudestưdidn {nr}')) 
                with c8:
                    kl=[]
                    for nr in range(rows):
                        kl.append(c8.selectbox('Kim loại ',["",'N',"Y"], key=f'dfqudestdidn1 {nr}')) 
                with  c1:
                    ks=[]
                    for nr in range(rows):
                        ks.append(c1.selectbox('',[kh_r[nr]], key=f'dfuesstidn {nr}'))
                with c2:
                    sap=[]
                    for nr in range(rows):
                        sap.append(c2.selectbox('',[sp[nr]], key=f'dfuestissdn {nr}'))
                st.form_submit_button('Submit')
                
                dict={"LỆNH SX":lsx,"NMSX":nm,"SẢN PHẨM (C/M)":ldh,"GIA CÔNG (Y/N)":gc1,"V/E U/CONG (Y/N)":uc,"DÁN VNR (Y/N)":vn,"K/L ĐB (Y/N)":kl}
                dff=pd.DataFrame.from_dict(dict)
                lsx_info=dff.merge(df,how='left',on="LỆNH SX")
                a=lsx_info[["LỆNH SX","TÊN KHÁCH HÀNG",	"TÊN SẢN PHẨM TTF",	 "NMSX",	"SẢN PHẨM (C/M)",	"GIA CÔNG (Y/N)",	"V/E U/CONG (Y/N)",	"DÁN VNR (Y/N)",	"K/L ĐB (Y/N)"]]
                a
            if st.button('Push'):

                # Pull order_info
                lsx_info["MÀU SƠN"]=lsx_info["MÀU SƠN"].str.replace('NA','N/A ')
                lsx_info=lsx_info.astype(str)
#                 lsx_info=lsx_info[["LỆNH SX",	"TÊN KHÁCH HÀNG",	"TÊN SẢN PHẨM TTF",	"ĐVT",	"LOẠI GỖ",	"MÀU SƠN"	,"NỆM"	,"NGÀY XUẤT",	"GHI CHÚ"]]
                lsx_info=lsx_info[["LỆNH SX",	 "NMSX",	"SẢN PHẨM (C/M)",	"GIA CÔNG (Y/N)",	"V/E U/CONG (Y/N)",	"DÁN VNR (Y/N)",	"K/L ĐB (Y/N)","SỐ ĐƠN HÀNG",	"TÊN KHÁCH HÀNG",	"TÊN SẢN PHẨM TTF"  ,"LOẠI GỖ","NỆM",		"ĐVT",	"SỐ LƯỢNG",	"GHI CHÚ",		"MÀU SƠN"	,	"NGÀY XUẤT"	]]
                ws1 = gc.open("DSX2.1 - Lệnh sản xuất").worksheet("1. LENH SX")
                ws2 = gc.open("LSX - lưu trữ").worksheet("LSX ĐÃ IN")
                push_lsx(lsx_info, ws1, ws2)

        if select=="CẬP NHẬT LẠI LSX CŨ":
            list_sdh=st.multiselect("Nhập số đơn hàng",lsx_cu["LỆNH SX"].unique().tolist())
            with st.form(key='columns_in_form'):

                if not list_sdh:
                    st.info('Nhập SĐH')
                else:

                    c0,c1,c2,c3,c4,c5,c6=st.columns((1.8,2,3,1,.9,.9,3))

                    df=lsx_cu[lsx_cu["LỆNH SX"].isin(list_sdh)].reset_index(drop=True)

                    list_r=df["LỆNH SX"].tolist()
                    kh_r=df["TÊN KHÁCH HÀNG"].tolist()  
                    sp=df["TÊN SẢN PHẨM TTF"].tolist()
                    SL=df['SỐ LƯỢNG'].tolist()
                    GO=df['LOẠI GỖ'].tolist()
                    SON=df['MÀU SƠN'].tolist()
                    # cols = st.beta_columns(5)
                    # for i, col in enumerate(cols):
                    rows = len(list_r)


                    # list_r=[50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200]
                    with c0:
                        lsx=[]
                        for nr in range(rows):
                            lsx.append(c0.selectbox('',[list_r[nr]], key=f'dfuestidn {nr}'))
                        # st.selectbox('Lệnh sản xuất',['a','b','c'])
                    with c3:
                        ldh=[]
                        for nr in range(rows):
                            ldh.append(c4.text_input('Loại Gỗ',GO[nr], key=f'dfquesatdidn {nr}'))  
                    with c4:
                        gc1=[]
                        for nr in range(rows):
                            gc1.append(c5.text_input('SỐ LƯỢNG ',SL[nr], key=f'dfqudesưtdidn {nr}')) 
                    with c5:
                        uc=[]
                        for nr in range(rows):
                            uc.append(c6.text_input('MÀU SƠN ',SON[nr], key=f'dfqudesưtdidn{nr}')) 
                    with  c1:
                        ks=[]
                        for nr in range(rows):
                            ks.append(c1.selectbox('',[kh_r[nr]], key=f'dfuesstidn {nr}'))
                    with c2:
                        sap=[]
                        for nr in range(rows):
                            sap.append(c2.text_input('',sp[nr], key=f'dfuestissdn {nr}'))
                st.form_submit_button('Submit')
                    
            dict={"LỆNH SX":lsx,"LOẠI GỖ":ldh,"SỐ LƯỢNG":gc1,'MÀU SƠN':uc,"TÊN SẢN PHẨM TTF":sap}
            dff=pd.DataFrame.from_dict(dict)
            df=df.drop(columns=["TÊN SẢN PHẨM TTF","LOẠI GỖ",'SỐ LƯỢNG','MÀU SƠN','Unnamed: 0'])
            lsx_info=dff.merge(df,how='left',on="LỆNH SX")
            a=lsx_info.drop(columns='Versionn') #[["LỆNH SX","TÊN KHÁCH HÀNG",	"TÊN SẢN PHẨM TTF"]]


            a['Versionn']=df['Versionn'].astype(int)+1

            if st.button('Xuất danh sách!'):
                lsx_info=a[["LỆNH SX",	 "NMSX",	"LOẠI GỖ",'SỐ LƯỢNG','MÀU SƠN',	"SỐ ĐƠN HÀNG",	"TÊN KHÁCH HÀNG",	"TÊN SẢN PHẨM TTF"  ,	"ĐVT",	"NGÀY XUẤT",	"GHI CHÚ",'Versionn']]
                ws1 = gc.open("DSX2.1 - Lệnh sản xuất").worksheet("1. LENH SX")
                ws2 = gc.open("LSX - lưu trữ").worksheet("LSX ĐÃ IN")
                lsx_info=lsx_info.astype(str)
                push_lsx_ver2(lsx_info,ws1,ws2)
                st.markdown("")
                tmp_download_link = download_link(lsx_info, 'YOUR_DF.csv', 'Bấm vào đây để tải danh sách!')
                st.markdown(tmp_download_link, unsafe_allow_html=True)





    if  password==st.secrets["password"] and username==st.secrets['use']:
        st.write('Goodjob!')
        #,'Bộ phận':["PKTH","QLCL","NM1","NM3","NM5","THU MUA","T.KH","TỔ KỸ THUẬT SƠN"],"Số lượng":[1,2,6,6,6,2,1,1]
        form=pd.DataFrame({'Tên tài liệu':['Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX','Lệnh sản xuất - LSX']})
        df2=df_df[1]
        data=df2[['LỆNH SX','SỐ ĐƠN HÀNG',"NMSX",'TÊN KHÁCH HÀNG','TÊN SẢN PHẨM TTF','SỐ LƯỢNG','LOẠI GỖ']]
        list_dh=data['SỐ ĐƠN HÀNG'].unique().tolist()
        colum1,colum2,clll3=st.columns((1,1,1))
        with colum1:
            list_sdh=st.multiselect("Nhập số đơn hàng",list_dh)
        with colum2:
            versionlsx=st.text_input('Version LSX:',)
        with clll3:
            cate=st.multiselect('Loại thông tin:',['LSX','TTSP+LSX','TTSP','LSX + TTSP + BVA4'])
        df=data[data["SỐ ĐƠN HÀNG"].isin(list_sdh)]
        list_r=df["LỆNH SX"].tolist()
        with st.form(key='columns_in_form'):
            a=st.multiselect('Các mã LSX cần photo TTSP:',list_r)      
            st.form_submit_button('Xác nhận')
        table=pd.DataFrame(a,columns=['LỆNH SX'])
        table['Version LSX']=versionlsx
        table['Loại thông tin']=cate[0]
        table=table.merge(data,how='left',on='LỆNH SX')

        table=table.rename(columns={'NMSX':5})
        tab=table.melt(id_vars=['LỆNH SX','SỐ ĐƠN HÀNG','TÊN KHÁCH HÀNG','TÊN SẢN PHẨM TTF','SỐ LƯỢNG','LOẠI GỖ','Version LSX','Loại thông tin'],value_name='Bộ phận')
        tab=tab.drop(columns={'variable'})
        tabb=tab.copy() # .merge(form,how='left',on='Bộ phận')
        tabb['Ngày']=datetime.date.today()
        tabb=tabb[['LỆNH SX','Version LSX','Bộ phận','Loại thông tin','Ngày']]
        tabb=tabb.astype(str)
        tabb
        if st.button('Xuất danh sách!'):
            ws1 = gc.open("TCHC - Theo dõi Photocopy").worksheet("Trang tính10")
            ws2 = gc.open("TCHC - Theo dõi Photocopy").worksheet("TD CHUYỂN GIAO TTSP TKH")
            push_lsx(tabb,ws1,ws2)




    if  password==st.secrets["pkth_pw"] and username==st.secrets['pkth_user']:    
        st.write('Goodjob!')
        with st.form(key='columns_in_form'):
            df2=df[1]
            data=df2[['LỆNH SX','SỐ ĐƠN HÀNG',"NMSX",'TÊN KHÁCH HÀNG','TÊN SẢN PHẨM TTF','SỐ LƯỢNG','LOẠI GỖ']]
            list_dh=data['LỆNH SX'].unique().tolist()
            list_sdh=st.multiselect("Nhập mã LSX",list_dh)
            st.form_submit_button('Xác nhận')
            table=pd.DataFrame(list_sdh,columns=['LỆNH SX'])
            table['NGÀY']=datetime.date.today()
        table
        if st.button('Xuất danh sách!'):
            ws1 = gc.open("CHECK LSX - HÀNG NGÀY").worksheet("Sheet1")
            ws2 = gc.open("CHECK LSX - HÀNG NGÀY").worksheet("Sheet2")
            push_lsx(table,ws1,ws2)
