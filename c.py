import psycopg2, csv, time
#
# This script reads data from the intermediate text csv file created by b.py
# and writes it to a table inside a postgres database
# Credit to Chad Cooper/NBI 
#
start = time.clock()

con = psycopg2.connect(database="NBI_DB", user="userdoug", password="postgres")
cur = con.cursor()
csvObject = csv.reader(open('masscsv', 'rb'),  delimiter = ',') 
#               3+42x3+2 = 131
passData = "INSERT INTO test2 (STATE,STRUC_NO,REC_TYPE, \
		RT_SIG_PFX,LVL_SERV,RT_NO, \
		DIR_SUFFIX,HWY_AG_DST,COUNTY, \
		PLACE_CODE,FEAT_INTSC,CRT_FAC_IN, \
		FAC_CAR_ST,LOCATION,MIN_VT_CLR, \
		KM_POINT,HWY_NETWK,LRS_ROUTE, \
		SUBRT_NO,LAT_DMS,LAT_DEG, \
		LAT_MIN,LAT_SEC,LAT_DD, \
		LON_DMS,LON_DEG,LON_MIN, \
		LON_SEC,LON_DD,BYPASS_LEN, \
		TOLL,MAINT_RESP,OWNER, \
		FUNCT_CLA,YEAR_BUILT,LANES_ON, \
		LANES_UNDR,AVG_TRAFIC,YR_AVG_TRF, \
		DSGN_LOAD,APRCH_WIDH,BRDG_MEDN, \
		SKEW,STRX_FLARD,BRDG_RAILS, \
		TRANSTNS,APRCH_RAIL,APRCH_R_ED, \
		HIST_SIGNF,NAV_CTRL,NAV_VT_CLR, \
		NAV_HZ_CLR,STRX_OPEN,SRV_TYP_ON, \
		SRV_TYP_UN,DSGN_TYPE,MAT_TYPE, \
		MAT_KIND,CNST_TYPE,NO_MN_SPAN, \
		NO_AP_SPAN,TOT_HZ_CLR,MX_SPN_LEN, \
		STRX_LEN,L_CURB_LEN,R_CURB_LEN, \
		RDWY_WIDTH,DECK_WIDTH,MN_V_CLR_O, \
		O_REF_FEAT,MN_V_CLR_U,U_REF_FEAT, \
		MN_LAT_UC,MN_LAT_UCL,DECK, \
		SUPERSTRX,SUBSTRX,CHAN_PROT, \
		CULVERTS,OP_RT_METH,OPER_RATE, \
		IV_RT_METH,INVEN_RATE,STRX_EVAL, \
		DECK_GEOM,UC_VT_HZ,BRDG_POST, \
		WW_ADEQCY,AP_RW_ALGN,PRP_WK_TYP, \
		WK_DONE_BY,IMPVMT_LEN,INSPECT_DT, \
		INSPT_FREQ,FRX_DET,UDWAT_INSP, \
		OT_INSP,FRX_DET_DT,UD_INSP_DT, \
		OT_INSP_DT,BRG_IMP_CT,RW_IMP_CT, \
		TOT_COST,YR_IMP_EST,NGHB_ST_CD, \
		PCT_RESP,BD_BRG_NO,STRAHNET, \
		STRX_DESIG,TRAFIC_DIR,TMP_STR_DS, \
		HY_SOI_RT,FD_LND_HY,YR_RECONST, \
		DK_ST_TYPE,WR_SF_TYPE,MEMB_TYPE, \
		DK_PROTCT,AVG_TRCK_C,NAT_NETWRK, \
		PIER_PROT,NBIS_LEN,SCR_BRDGS, \
		FUT_ADT,YR_FUT_ADT,MN_NAV_CLR, \
		FED_AGNCY,WASH_USE,STATUS, \
		ASTERISK,SUFF_RATNG) \
		VALUES (%s,%s,%s, \
		%s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s,%s, \
                %s,%s);" 
#
for row in csvObject:  
	csvLine = row       
	cur.execute(passData, csvLine) 
con.commit()
	
end = time.clock()
msg = '\n\n' + str(time.strftime('%I:%M:%S %p', time.localtime())) + \
              ':  Done! Process completed in '        
print msg   
print (end-start)
#      except:
#          tb = sys.exc_info()[2]
#      f       tbinfo = traceback.format_tb(tb)[! ]
#         pymsg = 'TrreplreplIathalon ERRORS:\nTraceback replInfo:\n' + tbinfo + \
#                  '\nError Info:\n    ' +  str(sys.exc_type)+ ': ' + \
#                  str(sys.exc_value) + '\n'
#         print pymsg
                
#        msgs = 'GP ERRORS:\n' + gp.GetMessages(2) + '\n'
#        print msgs
        
