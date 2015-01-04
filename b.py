import csv, time
# 
# This script processes the raw undelimited NBI data available at http://www.fhwa.dot.gov/bridge/nbi/ascii.cfm
# The raw data is parsed, in some cases functions are called which add information, such as translating the state
# code into an actual state name, translating the route prefix into it's text description (3='State highway')
# etc.
# The output of this script is an intermediate comma-separated-value text file. A separate script is then used to 
# write that text data into a postgres database table.
# Credit for this code goes to Chad Cooper.  This script is intended to be a non-ArcGIS alternative.
#
start = time.clock()
processedlinecount = 0
inputlinecount = 0
#
#	Create the state dictionary Note: Need to take this to a separate routine 
#	since it doesn't need to be run every time this is
#
reader = csv.reader(open('US_state_FIPS.csv', 'r'))
state = {}
for row in reader:
	   a, k, v = row
	   state[k] = v
#          print state

#
#	Create the county dictionary Note: Need to take this to a separate routine 
#	since it doesn't need to be run every time this is
#
reader = csv.reader(open('US_county_FIPS.csv', 'r'))
county= {}
for row in reader:
	   a, b, c, d, e, f, g = row
	   county[(b,d)] = c
#          print county 
#
#	Functions to assist with processing
#
def SimpleRecord(rowvalue):
	if rowvalue=='':
		rowvalue='NR'
#	
def get_route_prefix( rt_prefix_code ):
    """
    Translates the route signing prefix numeric code to a valid
    description via a dictionary
    """
    rd = {1:'Interstate highway',
          2:'U.S. numbered highway',
          3:'State highway',
          4:'County highway',
          5:'City street',
          6:'Federal lands road',
          7:'State lands road',
          8:'Other'}
    if int(rt_prefix_code) in rd.keys():
        rt_type = rd[int(rt_prefix_code)]
    else:
        rt_type = 'Unknown'
    return rt_type

def get_level_service( svc_code ):
    """
    Translates the designated level of service code to a valid
    description via a dictionary
    """
    sd = {0:'Other',
          1:'Mainline',
          2:'Alternate',
          3:'Bypass',
          4:'Spur',
          6:'Business',
          7:'Ramp/Wye/Connector/etc.',
          8:'Service and/or unclassified frontage road'}
    if int(svc_code) in sd.keys():
        svc_level = sd[int(svc_code)]
    else:
        svc_level = 'Unknown'
    return svc_level

def get_direction( directional_code ):
    """
    Translates the directional suffix code to a valid description via a 
    dictionary
    """
    dir_d = {0:'Not applicable',
             1:'North',
             2:'East',
             3:'South',
             4:'West'}
    if int(directional_code) in dir_d.keys():
        direction = dir_d[int(directional_code)]
    else:
        direction = 'Unknown'
    return direction

##def get_place( place_code ):
##    """
##    Translates the concatenation of the state FIPS code and FIPS place code
##    to a valid FIPS place name
##    """
##    if place_code in fips_place_dict.fips_places.keys():
##        place_name = fips_place_dict.fips_places[place_code]
##    elif place_code[2:] == '00000':
##        place_name = 'Not applicable'
##    else:
##        place_name = 'Unknown - ' + place_code[2:]
##    return place_name

def get_toll( toll_code ):
    """
    
    """
    toll_dict = {1:'Toll bridge',
                 2:'On toll road',
                 3:'On free road',
                 4:'On Interstate toll segment',
                 5:'Toll bridge separate from highway segment'}
    if int(toll_code) in toll_dict.keys():
        toll_name = toll_dict[int(toll_code)]
    else:
        toll_name = 'Unknown'
    return  toll_name

def get_funct_cls( funct_code ):
    """
    
    """
    if len(funct_code) < 1:
        funct_code = '00'
    funct_dict = {'01':'Rural - Principal Arterial - Interstate',
                  '02':'Rural - Principal Arterial - Other',
                  '06':'Rural - Minor Arterial',
                  '07':'Rural - Major Collector',
                  '08':'Rural - Minor Collector',
                  '09':'Rural - Local',
                  '11':'Urban - Principal Arterial - Interstate',
                  '12':'Urban - Principal Arterial - Other Freeway or Expressway',
                  '14':'Urban - Other Principal Arterial',
                  '16':'Urban - Minor Arterial',
                  '17':'Urban - Collector',
                  '19':'Urban - Local'}
    if funct_code in funct_dict.keys():
        funct_name = funct_dict[funct_code]
    else:
        funct_name = 'Unknown'
    return funct_name

def get_maint_resp( maint_code ):
    """
    
    """
    if len(maint_code) < 1:
        maint_code = 00
    maint_dict = {01:'State Highway Agency',
                  02:'County Highway Agency',
                  03:'Town or Township Highway Agency',
                  04:'City or Municipal Highway Agency',
                  11:'State Park/Forest/Reservation Agency',
                  12:'Local Park/Forest/Reservation Agency',
                  21:'Other State Agencies',
                  25:'Other Local Agencies',
                  26:'Private (other than railroad)',
                  27:'Railroad',
                  31:'State Toll Authority',
                  32:'Local Toll Authority',
                  60:'Other Federal Agencies (not listed below)',
                  61:'Indian Tribal Government',
                  62:'Bureau of Indian Affairs',
                  63:'Bureau of Fish and Wildlife',
                  64:'U.S. Forest Service',
                  66:'National Park Service',
                  67:'Tennessee Valley Authority',
                  68:'Bureau of Land Management',
                  69:'Bureau of Reclamation',
                  70:'Corps of Engineers (Civil)',
                  71:'Corps of Engineers (Military)',
                  72:'Air Force',
                  73:'Navy/Marines',
                  74:'Army',
                  75:'NASA',
                  76:'Metropolitan Washington Airports Service',
                  80:'Unknown'}
    if int(maint_code) in maint_dict.keys():
        maint_name = maint_dict[int(maint_code)]
    else:
        maint_name = 'Unknown'
    return maint_name

def get_bridge_median( median_code ):
    """
    
    """
    if len(median_code) < 1:
        median_code = 00
    med_dict = {0:'No median',
                1:'Open median',
                2:'Closed median (no barrier)',
                3:'Closed median with non-mountable barriers'}
    if int(median_code) in med_dict.keys():
        median_name = med_dict[int(median_code)]
    else:
        median_name = 'Unknown'
    return median_name
        
#	open raw data file and the write to file
#
#csvRaw = csv.reader(open('rawcsv', 'rb'),  delimiter = ',') 
rfile = open('CA13NoDelim.txt').read().splitlines()  # raw input file
wfile = open('masscsv','w')  # rewritten output file THIS NAME SHOULD JUST COME FROM THE INPUT FILE NAME 
inputlinecount=sum(1 for line in rfile if line.rstrip()) 
print 'Processing:'
print inputlinecount
print 'records from the input file'
#
# Start processing the raw file +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
try:
        for line in rfile:
	    processedlinecount = processedlinecount + 1
	    if processedlinecount%1000==0:
		    print 'Processed another thousand records'
	    try:
            	v0 = state[line[0:2].replace(',',' ')]   # 1 state (2) note this skips the FHWA region code
	    except KeyError, e:
		v0 = 'NR'
	    v1 = line[3:18].replace(',',' ').strip()       # structure number(15) 2 Highway Agency Dist, 3 County Code, 5 Place code
            v2 = line[18:19].replace(',',' ').strip()      # record type (1)
            v3 = get_route_prefix( line[19:20].
                                   replace(',',' ').strip() )  # rte prefix (1)
            v4 = get_level_service( line[20:21].
                                    replace(',',' ').strip() ) # level of svc (1)
            v5 = line[21:26].replace(',',' ').strip()      # route number (5)
            v6 = get_direction( line[26:27].
                                replace(',',' ').strip() ) # direct suffix (1)
            v7 = line[27:29].replace(',',' ').strip()      # hwy agency dist (2)
	    try:
                v8 = county[line[0:2].replace(',',' '), 
                             line[29:32].replace(',',' ')]  # county              
	    except KeyError, e:
		v8 = 'NR'
	    v9 = line[32:37].replace(',',' ').strip()      # place code  
            v10 = line[37:61].replace(',',' ').strip()     # 6 feat intersected
            v11 = line[61:62].replace(',',' ').strip()     # crit fac indic
            v12 = line[62:80].replace(',',' ').strip()     # fac carried
            v13 = line[80:105].replace(',',' ').strip()    # location            
            v14 = line[105:109].replace(',',' ').strip()   # min vert clrance
            v15 = line[109:116].replace(',',' ').strip()   # kilometerpoint
            v16 = line[116:117].replace(',',' ').strip()   # base hwy netwk
            v17 = line[117:127].replace(',',' ').strip()   # LRS invtry rte
            v18 = line[127:129].replace(',',' ').strip()   # subrte number
            v19 = line[129:137].replace(',',' ').strip()   # lat dms
            v20 = v19[0:2]                                 # lat deg
            v21 = v19[2:4]                                 # lat min
            v22 = v19[4:6]+'.'+v19[6:8]                    # lat sec
            v23 = str(int(v20)+(float(v21)/60)+(float(v22)/3600))  # lat dd
            v24 = line[137:146].replace(',',' ').strip()   # lon dms
            v25 = v24[0:3]                                 # lon deg
            v26 = v24[3:5]                                 # lon min
            v27 = v24[5:7]+'.'+v24[7:9]                    # lon sec
            v28 = str('-'+str(int(v25)+(float(v26)/60)+ \
                      float(v27)/3600))                    # lon dd
            # detour len conv from km to mi
            if len(line[146:149].replace(',',' ').strip()) > 0:
                v29 = '%.1f' % (float(line[146:149].
                      replace(',',' ').
                      strip())*0.62137)
            else:
                v29 = ''
            v30 = get_toll( line[149:150].
                            replace(',',' ').strip() )     # toll
            v31 = get_maint_resp( line[150:152].
                                  replace(',',' ').strip() )  # maint responsb  
            v32 = get_maint_resp( line[152:154].
                                  replace(',',' ').strip() )  # owner          
            v33 = get_funct_cls( line[154:156].
                                 replace(',',' ').strip() )   # funct class    
            v34 = line[156:160].replace(',',' ').strip()   # year built
            v35 = line[160:162].replace(',',' ').strip()   # lanes on
            v36 = line[162:164].replace(',',' ').strip()   # lanes under       <<<
            # avg traffic
            if len(line[164:170].replace(',',' ').strip()) > 0:
                v37 = '%d' % (int(line[164:170].
                      replace(',',' ').strip()))
            else:
                v37 = ''
            v38 = line[170:174].replace(',',' ').strip()   # yr of avg traffic
            v39 = line[174:175].replace(',',' ').strip()   # design load
            # approach width
            if len(line[175:179].replace(',',' ').strip()) > 0:
                v40 = '%.1f' % (float(str(int(line[175:179].
                      replace(',',' ').strip()[0:3]))+'.'+
                      line[175:179].strip()[3:4])*0.62137)
            else:
                v40 = ''
            v41 = get_bridge_median( line[179:180].
                                     replace(',',' ').strip() ) # bridge median
            v42 = line[180:182].replace(',',' ').strip()   # skew
            v43 = line[182:183].replace(',',' ').strip()   # strux flared
            v44 = line[183:184].replace(',',' ').strip()   # bridge railings
            v45 = line[184:185].replace(',',' ').strip()   # transitions
            v46 = line[185:186].replace(',',' ').strip()   # approach gd-rail
            v47 = line[186:187].replace(',',' ').strip()   # app gr-rail ends
            v48 = line[187:188].replace(',',' ').strip()   # historical sig (1)
            v49 = line[188:189].replace(',',' ').strip()   # navigation control (1)
            v50 = line[189:193].replace(',',' ').strip()   # navigation vert clr (4)
            v51 = line[193:198].replace(',',' ').strip()   # navigation horiz clr (5)
            v52 = line[198:199].replace(',',' ').strip()   # structure open, posted, closed (1)
            v53 = line[199:200].replace(',',' ').strip()   # type service  (1)
            v54 = line[200:201].replace(',',' ').strip()   # structure type main (1)
            v55 = line[201:202].replace(',',' ').strip()   # structure type approach (1)
            v56 = line[202:204].replace(',',' ').strip()   # 45 number of spans in main unit (2)
            v57 = line[204:205].replace(',',' ').strip()   # 46 number of approach spans (1)
            v58 = line[205:207].replace(',',' ').strip()   # inventory rte total horiz clr (3)
            v59 = line[207:210].replace(',',' ').strip()   # length of max span (3)
            v60 = line[210:214].replace(',',' ').strip()   # (4)
            v61 = line[214:217].replace(',',' ').strip()   # (3)
            v62 = line[217:222].replace(',',' ').strip()   # (5)
            v63 = line[222:228].replace(',',' ').strip()   # (6)
            v64 = line[228:231].replace(',',' ').strip()   # (3)
            v65 = line[231:234].replace(',',' ').strip()   # (3)
            v66 = line[234:238].replace(',',' ').strip()   # (4)
            v67 = line[238:242].replace(',',' ').strip()   # (4)
            v68 = line[242:246].replace(',',' ').strip()   # (4)
            v69 = line[246:247].replace(',',' ').strip()   # (1)
            v70 = line[247:251].replace(',',' ').strip()   # (4)
            v71 = line[251:252].replace(',',' ').strip()   # (1)
            v72 = line[252:255].replace(',',' ').strip()   # (3)
            v73 = line[255:258].replace(',',' ').strip()   # (3)
            v74 = line[258:259].replace(',',' ').strip()   # (1)
            v75 = line[259:260].replace(',',' ').strip()   # (1)
            v76 = line[260:261].replace(',',' ').strip()   # (1)
            v77 = line[261:262].replace(',',' ').strip()   # (1)
            v78 = line[262:263].replace(',',' ').strip()   # (1)
            v79 = line[263:264].replace(',',' ').strip()   # (1)
            v80 = line[264:267].replace(',',' ').strip()   # (3)
            v81 = line[267:268].replace(',',' ').strip()   # (1)
            v82 = line[268:271].replace(',',' ').strip()   # (3)
            v83 = line[271:272].replace(',',' ').strip()   # (1)
            v84 = line[272:273].replace(',',' ').strip()   # (1)
            v85 = line[273:274].replace(',',' ').strip()   # (1)
            v86 = line[274:275].replace(',',' ').strip()   # (1)
            v87 = line[275:276].replace(',',' ').strip()   # (1)
            v88 = line[276:277].replace(',',' ').strip()   # (1)
            v89 = line[277:279].replace(',',' ').strip()   # (2)
            v90 = line[279:280].replace(',',' ').strip()   # (1)
            v91 = line[280:286].replace(',',' ').strip()   # (6)
            v92 = line[286:290].replace(',',' ').strip()   # (4)
            v93 = line[290:292].replace(',',' ').strip()   # (2)
            v94 = line[292:295].replace(',',' ').strip()   # (3)
            v95 = line[295:298].replace(',',' ').strip()   # (3)
            v96 = line[298:301].replace(',',' ').strip()   # (3)
            v97 = line[301:305].replace(',',' ').strip()   # (4)
            v98 = line[305:309].replace(',',' ').strip()   # (4)
            v99 = line[309:313].replace(',',' ').strip()   # (4)
            v100 = line[313:319].replace(',',' ').strip()   # (6)
            v101 = line[319:325].replace(',',' ').strip()   # (6)
            v102 = line[325:331].replace(',',' ').strip()   # (6)
            v103 = line[331:335].replace(',',' ').strip()   # (4)
            v104 = line[335:338].replace(',',' ').strip()   # (3)
            v105 = line[338:340].replace(',',' ').strip()   # (2)
            v106 = line[340:355].replace(',',' ').strip()   # (15)
            v107 = line[355:356].replace(',',' ').strip()   # (1)
            v108 = line[356:357].replace(',',' ').strip()   # (1)
            v109 = line[357:358].replace(',',' ').strip()   # (1)
            v110 = line[358:359].replace(',',' ').strip()   # (1)
            v111 = line[359:360].replace(',',' ').strip()   # (1)
            v112 = line[360:361].replace(',',' ').strip()   # (1)
            v113 = line[361:365].replace(',',' ').strip()   # (4)
            v114 = line[365:366].replace(',',' ').strip()   # (1)
            v115 = line[366:367].replace(',',' ').strip()   # (1)
            v116 = line[367:368].replace(',',' ').strip()   # (1)
            v117 = line[368:369].replace(',',' ').strip()   # (1)
            v118 = line[369:371].replace(',',' ').strip()   # (2)
            v119 = line[371:372].replace(',',' ').strip()   # (1)
            v120 = line[372:373].replace(',',' ').strip()   # (1)
            v121 = line[373:374].replace(',',' ').strip()   # (1)
            v122 = line[374:375].replace(',',' ').strip()   # (1)
            v123 = line[375:381].replace(',',' ').strip()   # (6)
            v124 = line[381:385].replace(',',' ').strip()   # (4)
            v125 = line[385:389].replace(',',' ').strip()   # (4)
            v126 = line[390:391].replace(',',' ').strip()   # (1)
            v127 = line[391:426].replace(',',' ').strip()   # (33)
            v128 = line[426:427].replace(',',' ').strip()   # (1)
            v129 = line[427:428].replace(',',' ').strip()   # (1)
            v130 = line[428:432].replace(',',' ').strip()   # (4)

            newline = v0+','+v1+','+v2+','+v3+','+v4+','+v5+','+v6+','+ \
            v7+','+v8+','+v9+','+v10+','+v11+','+v12+','+v13+','+v14+','+ \
            v15+','+v16+','+v17+','+v18+','+v19+','+v20+','+v21+','+v22+','+ \
            v23+','+v24+','+v25+','+v26+','+v27+','+v28+','+v29+','+v30+','+ \
            v31+','+v32+','+v33+','+v34+','+v35+','+v36+','+v37+','+v38+','+ \
            v39+','+v40+','+v41+','+v42+','+v43+','+v44+','+v45+','+v46+','+ \
            v47+','+v48+','+v49+','+v50+','+v51+','+v52+','+v53+','+v54+','+ \
            v55+','+v56+','+v57+','+v58+','+v59+','+v60+','+v61+','+v62+','+ \
            v63+','+v64+','+v65+','+v66+','+v67+','+v68+','+v69+','+v70+','+ \
            v71+','+v72+','+v73+','+v74+','+v75+','+v76+','+v77+','+v78+','+ \
            v79+','+v80+','+v81+','+v82+','+v83+','+v84+','+v85+','+v86+','+ \
            v87+','+v88+','+v89+','+v90+','+v91+','+v92+','+v93+','+v94+','+ \
            v95+','+v96+','+v97+','+v98+','+v99+','+v100+','+v101+','+ \
            v102+','+v103+','+v104+','+v105+','+v106+','+v107+','+v108+','+ \
            v109+','+v110+','+v111+','+v112+','+v113+','+v114+','+v115+','+ \
            v116+','+v117+','+v118+','+v119+','+v120+','+v121+','+v122+','+ \
            v123+','+v124+','+v125+','+v126+','+v127+','+v128+','+v129+','+ \
            v130+'\n'
# v0 to v130 is 131 records            
            wfile.write(newline)
        wfile.close()
        
except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = 'PYTHON ERRORS:\nTraceback Info:\n' + tbinfo + \
                '\nError Info:\n    ' +  str(sys.exc_type)+ ': ' + \
                str(sys.exc_value) + '\n'
        print pymsg
                
        msgs = 'GP ERRORS:\n' + gp.GetMessages(2) + '\n'
        print msgs
        
#
end = time.clock()
msg = '\n\n' + str(time.strftime('%I:%M:%S %p', time.localtime())) + \
              ':  Done!' + '\n' +' Process completed in '        
print msg   
print (end-start)
print '\n' + 'Total lines in the input file:'
print inputlinecount 
print 'Total lines processed and written to the intermediate file/database:'
print processedlinecount
print '\n' 
print '\n' + 'The last input line processed was:'
print line
print '\n' + 'This line was modified by the script to be:'
print newline
print '\n\n'
