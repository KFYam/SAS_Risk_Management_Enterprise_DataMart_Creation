/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* x "mv /NCB/NCB_Basel/SAS_ETL/13_FactTable/ddm*.* /NCB/NCB_Basel/SAS_ETL/13_FactTable/obsolete";	*/ 
/* ************************************************************************************************ */
%macro concat_DDM;
	%let tards=stg_ddm.ddm_combine_all_&st_RptMth.;
/*	%let tards=work.ddm_combine_all;*/
	/* Start : Checking any multi-length of same field name across the months */
	data ddm_fieldlists;
		set sashelp.vcolumn;
		where 	libname="RAW_DDM" and 
				substr(memname,1,8)="RM1DDMON" and 
			  	input(substr(memname,10,6)||"01",yymmdd8.) <= &dt_RptMth. and
			  	upcase(strip(type))="CHAR";
	run;
	proc sql noprint;
		create table multfieldlength as
		select name, max(length) as max_length, count(1) as cnt 
		from (select distinct name, length from ddm_fieldlists where upcase(strip(type))="CHAR")
		group by name having count(1) > 1 ;
	quit;
	/* end : Checking any multi-length of same field name across the months */
	data _null_;
		file "&dir_tmp./ddm_cat&st_TS..dat" lrecl=65535;
		put "data &tards.;";
	run;
	/* start: re-declare the length for field names which have multi-length across the months */
	data _null_;
		file "&dir_tmp./ddm_cat&st_TS..dat" mod;
		set multfieldlength ;
		new_name=catt("'",name,"'n");
		new_length=compress("$"||put(max_length,best32.)," ");
		put "length " new_name new_length ";";	
	run;
	/* end: re-declare the length for field names which have multi-length across the months */
	data _null_;
		file "&dir_tmp./ddm_cat&st_TS..dat" mod;
		put "set";
	run;
	data _null_;
		file "&dir_tmp./ddm_cat&st_TS..dat" mod;
		set sashelp.vtable;
		where 	libname="RAW_DDM" and 
				substr(memname,1,8)="RM1DDMON" and 
			  	input(substr(memname,10,6)||"01",yymmdd8.) <= &dt_RptMth.;
		tmp=strip(libname)||"."||strip(memname);
		put tmp;
	run;
	data _null_;
		file "&dir_tmp./ddm_cat&st_TS..dat" mod;
		put "; run;";
	run;
	%include "&dir_tmp./ddm_cat&st_TS..dat" /source2;
%mend;

%macro transpose_Default();

	proc format; value $m_id "Y"="CHN" "N"="HKG"; run;

	data stg_ddm.ddm_tx_master_&st_RptMth.;
		set stg_ddm.ddm_combine_all_&st_RptMth.;
		length key_generic $1000;
		length key_cin 	$255;
		length key_id	$255;
		length key_acno	$255;
		length n_default_type 8 d_default_start_date 8 d_default_end_date 8 sasflag_exceptional $100;
		n_orig_seq=_n_;
		key_generic=cats(
							c_ind_mainland, 
							strip(put(n_svid,best32.)), 
							strip(put(n_bk,best32.)),
							strip(put(n_cin,best32.)),
							strip(put(n_acno,best32.)),
							strip(put(n_lsno,best32.)),
							c_refno,
							c_cons_no, 
							c_issuer_code,
							c_security_code, 
							c_book_code,
							c_position_code,
							c_apl, 
							strip(put(n_gds_prd_typ,best32.)),
							c_ccy,
							c_cust_id
						);
		key_cin		=catx("-",strip(put(c_ind_mainland,$m_id.)),strip(put(n_cin,z11.)));
		key_acno	=catx("-",strip(put(c_ind_mainland,$m_id.)),strip(put(n_acno,z14.)));	 
		key_id		=catx("_",put(c_ind_mainland,$m_id.), strip(c_cust_id));
		if missing(n_cin) or n_cin = 0		then key_cin="";
		if missing(n_acno) or n_acno = 0	then key_acno="";
		if missing(c_cust_id)				then key_id="";

		if	c_customer_type ne 'T10' and 
			substr(c_cust_id,1,1) ne '1' and /* HKID */
			substr(c_cust_id,1,2) ne '92' /* other - HKID */
			and c_apl ne 'CRD' 
		then  n_ind_non_retail = 1;

		array def1 {2, 50} /*row x column*/	d_fdate_1-d_fdate_50		d_tdate_1-d_tdate_50;
		array def2 {2, 3} /*row x column*/	d_d2_fdate_1-d_d2_fdate_3	d_d2_tdate_1-d_d2_tdate_3;
		array def3 {2, 3} /*row x column*/	d_d3_fdate_1-d_d3_fdate_3	d_d3_tdate_1-d_d3_tdate_3;
		array def4 {2, 3} /*row x column*/	d_d4_fdate_1-d_d4_fdate_3	d_d4_tdate_1-d_d4_tdate_3;

		/* start - sense checking logic */
		if sum(	of d_fdate_1	-d_fdate_50,		
				of d_d2_fdate_1	-d_d2_fdate_3, 
				of d_d3_fdate_1	-d_d3_fdate_3,	
				of d_d4_fdate_1	-d_d4_fdate_3) = . then do;
			n_default_type = .;
			sasflag_exceptional="999";
			output;
		end;
		/* end - sense checking logic */
		%do type=1 %to 4;
		do i=1 to dim2(def&type.);
			n_default_type = &type.;
			/* start - sense checking logic */
			if 	missing(def&type.(1,i)) and not missing(def&type.(2,i)) then sasflag_exceptional="&type." ;
			/* end - sense checking logic */
			if 	not missing(def&type.(1,i)) or not missing(def&type.(2,i)) then do;
				d_default_start_date = def&type.(1,i);
				d_default_end_date = def&type.(2,i);
				output;
			end;
		end;
		%end;
		
		drop d_fdate_: d_tdate_: ;
		drop d_d2_fdate_: d_d2_tdate_: ;
		drop d_d3_fdate_: d_d3_tdate_: ;
		drop d_d4_fdate_: d_d4_tdate_: ;
		drop i;
		format d_default_: yymmdds10.;
	run;
	proc sort data=stg_ddm.ddm_tx_master_&st_RptMth.;
		by key_generic n_default_type d_default_start_date d_ac_date d_default_end_date;
	run;
%mend;

%macro gen_FactDDM();
	/* Lookup of sasflag_exceptional 
	sasflag_exceptional = 999 : when all the default start dates are missing but existing in DDM file
	sasflag_exceptional = 1 : when default type 1 start dates is missing while default type 1 end date is not missing. 
	sasflag_exceptional = 2 : when default type 2 start dates is missing while default type 2 end date is not missing. 
	sasflag_exceptional = 3 : when default type 3 start dates is missing while default type 3 end date is not missing. 
	sasflag_exceptional = 4 : when default type 4 start dates is missing while default type 4 end date is not missing. 
	sasflag_exceptional = 11 : Under the same default type across months, the default start date is unchanged while the default end date is changed to missing
	sasflag_exceptional = 12 : Under the same default type across months, the default start date is unchanged while the default end date is changed to another date value.
	sasflag_exceptional = 13 : Last default end date is later than the next default start date under same default type.
								using below example as illustration:
								D1 1st default: from 2012/1/1 to 2012/5/30
	 							D1 2nd default: from 2012/4/12 to 2012/7/21
	sasflag_exceptional = 21 : default start date is reversely changed across the months.
	*/
	data fact.ddm_tx_master_allhist_&st_RptMth.;
		set stg_ddm.ddm_tx_master_&st_RptMth.;
		by key_generic n_default_type d_default_start_date d_ac_date d_default_end_date;

		lag_defenddate		= lag(d_default_end_date);
		lag_defstartdate	= lag(d_default_start_date);
		lag_acdate 			= lag(d_ac_date);

		/* start - sense checking logic */
		if first.d_default_start_date and not lag_acdate  > d_ac_date then
			sasflag_exceptional = catx(",",sasflag_exceptional,"21");

		if first.d_default_start_date ne last.d_default_start_date then do;
			/* the first of d_default_start_date would not be checked */
			if not first.d_default_start_date then do;
				if not missing(lag_defenddate) and missing(d_default_end_date) then 
					sasflag_exceptional = catx(",",sasflag_exceptional,"11");
				if not missing(lag_defenddate) and d_default_end_date ne lag_defenddate then 
					sasflag_exceptional = catx(",",sasflag_exceptional,"12");
			end;
		end;

		if first.n_default_type ne last.n_default_type then do;
			if not first.n_default_type then do;
				if first.d_default_start_date then do;
					if not missing(lag_defenddate) and d_default_start_date <= lag_defenddate then
						sasflag_exceptional = catx(",",sasflag_exceptional,"13");
				end;
			end;
		end;
		/* end - sense checking logic */
		format lag_defenddate lag_defstartdate yymmdds10.;
		drop lag_:;
	run;
	/*
	proc datasets library = fact noprint;
		modify ddm_tx_master_allhist;
		index create n_cin;
		index create n_acno;
		index create d_ac_date;
		index create n_default_type;
		index create d_default_start_date;
		index create d_default_end_date;
		index create key_cin;
		index create key_acno;
		index create key_id;
	run;
	*/
%mend;
%macro gen_FactDDM_Main();
	%if %sysfunc(exist(fact.ddm_tx_master_allhist_&st_RptMth.)) %then %do;
		%put ---- NCB LOG ---- : !! fact.ddm_tx_master_allhist_&st_RptMth. is already existed.;
	%end;
	%else %do;
		%concat_DDM;
		%transpose_Default;
		%gen_FactDDM;	
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=DDM_&st_RptMth._CONCAT_&st_cltinfo.);
%gen_FactDDM_Main;
%exportlog(ind=STOP);
