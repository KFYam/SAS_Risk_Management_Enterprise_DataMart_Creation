/* ************************************************************************************* */
/* For BANK PD */
/* ************************************************************************************* */
%macro gen_BANK_Exclusion(stdte=, rmp=);
	%let SRC 	= mart.&RMP._&stdte.;
	%let OUT	= mart.&RMP._&stdte.;
	%let dtdte	= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));
	data &out.;
		set &src.;
		length sasflag_dq_excluded 8;
		call missing(sasflag_dq_excluded);
		if missing(sasflag_dq_excluded) and not missing(sasflag_intra_bank_group)	then sasflag_dq_excluded = 0.1;
		/*
		* As BOCHK CMM includes EAD=0 cases for performance report, it would not exclude; 
		if missing(sasflag_dq_excluded) and missing(ead_pre_ccf) 					then sasflag_dq_excluded = 1.2;
		if missing(sasflag_dq_excluded) and ead_pre_ccf = 0							then sasflag_dq_excluded = 1.3;
		if missing(sasflag_dq_excluded) and ead_pre_ccf < 0 						then sasflag_dq_excluded = 1.4;
		*/
		if missing(sasflag_dq_excluded) and . < d_next_review_date <= &dtdte.		then sasflag_dq_excluded = 2;
		if missing(sasflag_dq_excluded) and index(c_final_rating_irs,"8") > 0  		then sasflag_dq_excluded = 4.1;
		if missing(sasflag_dq_excluded) and index(car_rating,"8") > 0 				then sasflag_dq_excluded = 4.2;
		/*BOCHK CMM did not put incompleted FS checking when preparing performance report*/
		/*if missing(sasflag_dq_excluded) and upcase(strip(c_ind_incompleted_fs))="Y"	then sasflag_dq_excluded = 5;*/
	run;
%mend;
%gen_BANK_Exclusion(stdte=&st_BackMth.,	rmp=RMP2BIOBANK);
%gen_BANK_Exclusion(stdte=&st_RptMth.,	rmp=RMP2BIOBANK);
