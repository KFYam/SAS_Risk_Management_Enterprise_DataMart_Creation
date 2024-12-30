/* ************************************************************************************* */
/* For LC PD */
/* ************************************************************************************* */
%macro gen_LC_Exclusion(stdte=, rmp=);
	%let SRC 	= mart.&RMP._&stdte.;
	%let OUT	= mart.&RMP._&stdte.;
	%let dtdte	= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));
	data tmp_rmp;
		set &src.;
		length sasflag_dq_excluded 8;
		call missing(sasflag_dq_excluded); 		
		/* Changed on 23 Dec 2016, including those CIN with the change of model name (such as from BANK-like Model to Large Corporate Model) */
		if missing(sasflag_dq_excluded) and not missing(car_model) and strip(car_model) ne "Large Corporate Model" then sasflag_dq_excluded = .S;
		if missing(sasflag_dq_excluded) and sasflag_rsme_ind = 1 					then sasflag_dq_excluded = 1.1;
		if missing(sasflag_dq_excluded) and missing(ead_pre_ccf) 					then sasflag_dq_excluded = 1.2;
		if missing(sasflag_dq_excluded) and ead_pre_ccf = 0							then sasflag_dq_excluded = 1.3;
		if missing(sasflag_dq_excluded) and ead_pre_ccf < 0 						then sasflag_dq_excluded = 1.4;
		if missing(sasflag_dq_excluded) and . < d_next_review_date <= &dtdte.		then sasflag_dq_excluded = 2;
		/* funding vehicle is not applicable on LC model */
		/*if missing(sasflag_dq_excluded) and upcase(strip(c_ind_fv_indicator))="Y" then sasflag_dq_excluded = 3;*/
		if missing(sasflag_dq_excluded) and index(c_final_rating,"8") > 0  			then sasflag_dq_excluded = 4.1;
		if missing(sasflag_dq_excluded) and index(car_rating,"8") > 0 				then sasflag_dq_excluded = 4.2;
		if missing(sasflag_dq_excluded) and upcase(strip(c_ind_incompleted_fs))="Y"	then sasflag_dq_excluded = 5;
	run;
	%gen_AllocatedEAD(src=tmp_rmp,tar=&out.);

%mend;
%gen_LC_Exclusion(stdte=&st_BackMth.,	rmp=RMP2BIOLCM);
%gen_LC_Exclusion(stdte=&st_RptMth.,	rmp=RMP2BIOLCM);
