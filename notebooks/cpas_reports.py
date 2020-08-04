"""
Extract grant reports from CPAS
"""
import os

import pandas
import sqlalchemy


USERNAME = os.environ["CPAS_USERNAME"]
PASSWORD = os.environ["CPAS_PASSWORD"]
HOSTNAME = "10.32.196.224"
PORT = "1332"
SERVICE_NAME = "visispd1.lacity.org"

engine = sqlalchemy.create_engine(
    f"oracle+cx_oracle://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/"
    f"?service_name={SERVICE_NAME}"
)

def current_year():
    sql = """
    SELECT
        yr.strt_dt, 
		yr.end_dt, 
        yr.yr_desc, 
		yr.grnt_yr_id, 
		yr.current_request_yr, 
        TO_CHAR(yr.SUBM_EXPIR_DT,'mm/dd/yyyy hh24:mi:ss') AS SUBM_EXPIR_DT
	FROM GRNT.grnt_yr yr
	WHERE current_request_yr=1
    """
    return pandas.read_sql(sql, engine)

def all_year_list():
    sql = """
    SELECT
        yr.strt_dt, 
		yr.end_dt, 
		yr.yr_desc, 
		yr.grnt_yr_id, 
		yr.current_request_yr, 
		TO_CHAR(yr.SUBM_EXPIR_DT,'mm/dd/yyyy hh24:mi:ss') AS SUBM_EXPIR_DT
    FROM GRNT.grnt_yr yr
    """
    return pandas.read_sql(sql, engine)
    
def get_application_info(year):
    sql = f"""
    SELECT
	'{year}' as pgm_year,
    proj_id,
    (
        SELECT nvl(sum(CDBG_FND_AMT),0)
        FROM GRNT{year}.grnt_pep gp  
        WHERE gp.GRNT_APLCTN_ID=gpj.grnt_aplctn_id
     ) as REIMBURSETOTAL,
    (
        SELECT nvl(sum(GRNT_APPROVED_AMT) ,0)
        FROM GRNT{year}.GRNT_PRJCT_FND 
        WHERE grnt_aplctn_id=GA.grnt_aplctn_id AND GRNT_L_FND=190 
    ) AS approvedcdbgtotal, 
    (
        SELECT nvl(sum(fnd_amt),0) from GRNT{year}.GRNT_SRC_FND 
        WHERE grnt_prjct_fnd_id IN
        (
            SELECT grnt_prjct_fnd_id
            FROM GRNT{year}.GRNT_PRJCT_FND
            WHERE grnt_aplctn_id=GA.grnt_aplctn_id AND grnt_l_fnd=190
        )
     ) AS cdbgRequested,
     GA.PRJCT_TTL,
     gpj.grnt_aplctn_id,
     BYPASS_EMAIL_NOTIF_YN ,
     PRPSD_CONTRACT_CNT,
     GRNT_L_PROJ_LOC_RSN,
     KNWN_SITE_LOC_YN,
     CNFID_YN,
     (
         SELECT count(1) from GRNT{year}.grnt_pep
         WHERE grnt_aplctn_id=gpj.grnt_aplctn_id AND GRNT_L_PEP_STTS in (476,477)
     ) as pepSubmittedCount,
    (
        SELECT count(1) from GRNT{year}.grnt_pep
        WHERE grnt_aplctn_id=gpj.grnt_aplctn_id AND GRNT_L_PEP_STTS=477 
     ) AS pepApprovedCount,
     (
        SELECT count(1)
        FROM GRNT{year}.GRNT_PRJCT_LOCTN
        WHERE grnt_aplctn_id=gpj.grnt_aplctn_id
    ) AS locationCount  
    FROM 
        GRNT{year}.GRNT_PRPSD_PROJ  gpj,
        GRNT{year}.GRNT_APLCTN    GA
    WHERE GPJ.GRNT_APLCTN_ID=GA.GRNT_APLCTN_ID
    AND ga.grnt_aplctn_id IN
    (
        SELECT grnt_aplctn_id 
        FROM GRNT{year}.grnt_aplctn, GRNT{year}.grnt_cntct cicon
        WHERE 
        grnt_aplctn.grnt_city_dept_cntct_id = cicon.grnt_cntct_id	
        AND
        ( 
            CICON.GRNT_L_DEPT=
            (
                SELECT GRNT_L_DEPT 
                FROM 
                    GRNT.GRNT_USER,
                    GRNT.GRNT_USER_DEPT
                WHERE
                    GRNT_USER.USER_ID=GRNT_USER_DEPT.USER_ID
                    AND GRNT_USER_DEPT.GRNT_YR_ID= {year}
                    -- AND GRNT_USER.USER_ID = ?
            )  
            OR 
            -- GA.GRNT_LOGIN_ID = ?
            1=1
        )
    )
    -- retreive only applications that have an approved cdbg amount
    AND ga.GRNT_APLCTN_ID IN 
    (
        SELECT GRNT_APLCTN_ID
        FROM GRNT{year}.GRNT_PRJCT_FND 
        WHERE GRNT_L_FND=190 and GRNT_APPROVED_AMT > 0
    )
    order by PRJCT_TTL
    """
    return pandas.read_sql(sql, engine)

def app_list(year):
    """
    Query the application list for a given grant year.
    Derived from CPAS -> Con-Plan Funds -> {year} -> View Application List
    
    Parameters
    ==========
    year: int
        The grant year, from 30 to 46.
    """

    sql = f"""
	SELECT * FROM  (
	    SELECT 
        	'{year}' as pgm_year,
			apl.grnt_aplctn_id, 
            apl.prjct_ttl, 
			apl.fnd_yr,
            (
                SELECT field_value 
				FROM 
					GRNT{year}.grnt_l_stat, 
					GRNT{year}.grnt_stat 
				WHERE 
                    grnt_stat.grnt_l_stat=grnt_l_stat.field_id
					AND apl.grnt_aplctn_id=grnt_stat.grnt_aplctn_id
            )  AS aplctn_status,
			-- DEPT AND DIVISION
			(
                SELECT field_value
                FROM 
                    grnt.grnt_L_dept,
					GRNT{year}.grnt_cntct  
				WHERE 
					grnt_cntct.grnt_L_dept = grnt_L_dept.field_id
					AND cty_dept_yn=1
					AND apl.grnt_city_dept_cntct_id = grnt_cntct_id		
			) as dept_nm
		FROM GRNT{year}.GRNT_APLCTN apl 
		WHERE 1=1	 
	)
	WHERE 1=1
	AND aplctn_status = 'SUBMITTED'
	AND  fnd_yr={year}
	ORDER BY prjct_ttl ,dept_nm ,aplctn_status   
    """
    return pandas.read_sql(sql, engine)
 
def get_pep_info(year):
    """
    Query the application list for a given grant year.
    Derived from CPAS -> Con-Plan Funds -> {year} -> View PEP Tracking Info
    
    Parameters
    ==========
    year: int
        The grant year, from 30 to 46.
    """
    
    sql = f"""
 	SELECT 
	    '{year}' as pgm_year,
        grnt_pep_id,
        PEP_AGCY_NM,
        PEP_PROJ_NM,
        CITYWIDE_CNCL_DIST_YN,
		GRNT_L_PEP_STTS,
        to_char(PEP_VRFTN_DT,'mm/dd/yyyy') as PEP_VRFTN_DT, 
        to_char(PEP_TO_ENVIRON_DT,'mm/dd/yyyy') as PEP_TO_ENVIRON_DT , 
        to_char(PEP_TO_MGMT_DT,'mm/dd/yyyy')  as PEP_TO_MGMT_DT, 
        to_char(PEP_SEND_OUT_DT,'mm/dd/yyyy') as PEP_SEND_OUT_DT, 
        GRNT_L_SEND_OUT_MTHD, 
        to_char(CDBO_SEND_VRFTN_DT,'mm/dd/yyyy') as CDBO_SEND_VRFTN_DT, 
        to_char(PEP_RCV_DT,'mm/dd/yyyy') as PEP_RCV_DT,
		(
            SELECT FIELD_VALUE from GRNT{year}.GRNT_L_PEP_TYP
            WHERE FIELD_ID=gp.GRNT_L_PEP_TYP) as GRNT_L_PEP_TYP_VALUE,
            (
                SELECT PRJCT_TTL from GRNT{year}.grnt_aplctn 
                WHERE grnt_aplctn_id=gp.grnt_aplctn_id
            ) AS PRJCT_TTL,
		CDBG_FND_AMT,
        (
            SELECT PROJ_ID from GRNT{year}.GRNT_PRPSD_PROJ 
            WHERE grnt_aplctn_id=gp.grnt_aplctn_id) as PROJ_ID,
            (
                SELECT pep_note from GRNT{year}.GRNT_PEP_NOTE
                WHERE GRNT_L_PEP_NOTE_TYP=470
                AND GRNT_PEP_ID=gp.grnt_pep_id
            ) as trackingcomment,
            (
                SELECT pep_note from GRNT{year}.GRNT_PEP_NOTE
                WHERE GRNT_L_PEP_NOTE_TYP=485
                AND GRNT_PEP_ID=gp.grnt_pep_id) as PEP_VRFTN_COMMENT, 
                (
                    SELECT pep_note from GRNT{year}.GRNT_PEP_NOTE
                    WHERE GRNT_L_PEP_NOTE_TYP=486
                    AND GRNT_PEP_ID=gp.grnt_pep_id
                ) AS PEP_TO_ENVIRON_COMMENT,
		
		(
            SELECT pep_note from GRNT39.GRNT_PEP_NOTE
            WHERE GRNT_L_PEP_NOTE_TYP=487
            AND GRNT_PEP_ID=gp.grnt_pep_id
        ) AS PEP_TO_MGMT_COMMENT,  
		(
            SELECT pep_note from GRNT{year}.GRNT_PEP_NOTE
            WHERE GRNT_L_PEP_NOTE_TYP=488
            AND GRNT_PEP_ID=gp.grnt_pep_id
        ) AS CDBO_SEND_VRFTN_COMMENT,
		(
            SELECT FIELD_VALUE from GRNT{year}.GRNT_L_PEP_STTS
            WHERE FIELD_ID= gp.GRNT_L_PEP_STTS
        ) AS GRNT_L_PEP_STTS_VALUE,
		(
            SELECT FIELD_VALUE from GRNT{year}.GRNT_L_SEND_OUT_MTHD
            WHERE FIELD_ID = gp.GRNT_L_SEND_OUT_MTHD
        ) AS GRNT_L_SEND_OUT_MTHD_VALUE,
		(
            SELECT ldp.field_value
            FROM 
                GRNT{year}.grnt_aplctn apl,  
                GRNT{year}.grnt_cntct cicon, 
                grnt.grnt_L_dept ldp 
            WHERE
                apl.grnt_aplctn_id =  gp.grnt_aplctn_id
                AND cicon.cty_dept_yn = 1 
                AND apl.grnt_city_dept_cntct_id = cicon.grnt_cntct_id 
                AND cicon.grnt_L_dept = ldp.field_id 
		) as department,
		GRNT_L_REC_COLOR,
		(
            SELECT FIELD_VALUE
            FROM GRNT{year}.GRNT_L_REC_COLOR
            WHERE FIELD_ID=gp.GRNT_L_REC_COLOR
        ) AS GRNT_L_REC_COLOR_VALUE, 
		(
            SELECT grnt_user.grnt_utility.VALUE_LIST('
					SELECT 
					  	field_value 
					FROM 
					  	GRNT{year}.GRNT_PEP_CNCL_DIST A,
						GRNT{year}.GRNT_L_CNCL_DIST B
					WHERE B.FIELD_ID=A.GRNT_L_CNCL_DIST
					AND
					grnt_pep_id =
				' || gp.grnt_pep_id  ) FROM dual
        ) as council_district,	
		temp.status_date
	FROM 
		GRNT{year}.grnt_pep  gp,
		(
            SELECT max(mod_dt) as status_date,pk_id
            FROM grnt38.GRNT_HIST
			WHERE 
				table_nm='GRNT_PEP'
				AND pk_nm='GRNT_PEP_ID' 
				AND field_nm='GRNT_L_PEP_STTS'
				GROUP BY pk_id
		) temp
	WHERE gp.grnt_pep_id=temp.pk_id(+)
	AND grnt_pep_id in (
		SELECT grnt_pep_id FROM 
		GRNT{year}.GRNT_PEP
	)		
	ORDER BY PROJ_ID,PEP_AGCY_NM
	"""
    return pandas.read_sql(sql, engine)


def get_grnt_gpr(year):
    USER_gpr = f"GRNT{year}"   
    sql = f"""
    SELECT
        '{year}' as pgm_year,
        {USER_gpr}.grnt_gpr.yr,
        {USER_gpr}.grnt_gpr.grnt_gpr_id,
        {USER_gpr}.grnt_gpr.dept,
        {USER_gpr}.grnt_gpr.pid,
        {USER_gpr}.grnt_gpr.actv_nbr,
        {USER_gpr}.grnt_gpr.proj_nm,
        {USER_gpr}.grnt_gpr.actv_nm,
        {USER_gpr}.grnt_gpr.proj_addr,
        {USER_gpr}.grnt_gpr.proj_desc,
        {USER_gpr}.grnt_gpr.natl_obj,
        {USER_gpr}.grnt_gpr.hud_cd,
        {USER_gpr}.grnt_hud_cd.grnt_hud_cd_id,
        {USER_gpr}.grnt_hud_cd.ttl,
        {USER_gpr}.grnt_hud_cd.regulation_cit,
        {USER_gpr}.grnt_gpr.grnt_l_accmplsh,
        {USER_gpr}.grnt_gpr.obj_cnt,
        {USER_gpr}.grnt_gpr.otcm_cnt,
        {USER_gpr}.grnt_gpr.accmplsh_actl_units,
        {USER_gpr}.grnt_gpr.accmplsh_narrtv,
        {USER_gpr}.grnt_gpr.fund_amt,
        {USER_gpr}.grnt_gpr.drn_thru_amt,
        {USER_gpr}.grnt_gpr.tot_accmplsh,
        {USER_gpr}.grnt_gpr.tot_hsg,
        {USER_gpr}.grnt_gpr.accmplsh_narrtv_updt,
        {USER_gpr}.grnt_gpr.aprv_anlst_email,
        {USER_gpr}.grnt_gpr.aprv_anlst_tel,
        {USER_gpr}.grnt_gpr.aprv_anlst_sig_dt,
        {USER_gpr}.grnt_gpr.aprv_supv_nm,
        {USER_gpr}.grnt_gpr.aprv_supv_email,
        {USER_gpr}.grnt_gpr.aprv_anlst_dept_nm,
        {USER_gpr}.grnt_gpr.gpr_subm_dt,
        {USER_gpr}.grnt_gpr.grnt_l_gpr_actv_stts,
        {USER_gpr}.grnt_gpr.ent_in_idis_dt
    FROM
        {USER_gpr}.grnt_gpr
        INNER JOIN {USER_gpr}.grnt_hud_cd ON {USER_gpr}.grnt_gpr.hud_cd = {USER_gpr}.grnt_hud_cd.hud_cd
    WHERE
        {USER_gpr}.grnt_gpr.yr > '2009'
    ORDER BY
        {USER_gpr}.grnt_gpr.yr,
        {USER_gpr}.grnt_gpr.proj_nm
    """
    return pandas.read_sql(sql, engine)

#print(get_application_info(38))	
#print(app_list(41))
#print(get_pep_info(41))
#print(all_year_list())
#print(current_year())