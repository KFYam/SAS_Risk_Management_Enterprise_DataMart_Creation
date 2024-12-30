/* ************************************************************************************* */
/* For MM PD */
/* ************************************************************************************* */
%macro gen_MM_Exclusion(stdte=, rmp=);
	
	%let SRC 	= stg_oth.&RMP._ALL_&stdte.;
	%let OUT	= mart.&RMP._ALL_&stdte.;
	%let KEYTBL	= mart.&RMP._KEY_&stdte.;
	%let dtdte	= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));

	data &out.;
		set &src.;
		length sasflag_dq_excluded 8;

		if missing(sasflag_dq_excluded) and strip(car_model) ne "MM (CSME) Model" and not missing(car_model) then 
			sasflag_dq_excluded = 1.1;
		if missing(sasflag_dq_excluded) and sasflag_rsme_ind = 1 					then sasflag_dq_excluded = 1.1;
		if missing(sasflag_dq_excluded) and missing(ead_pre_ccf) 					then sasflag_dq_excluded = 1.2;
		if missing(sasflag_dq_excluded) and ead_pre_ccf = 0							then sasflag_dq_excluded = 1.3;
		if missing(sasflag_dq_excluded) and ead_pre_ccf < 0 						then sasflag_dq_excluded = 1.4;
		if missing(sasflag_dq_excluded) and . < d_next_review_date <= &dtdte.		then sasflag_dq_excluded = 2;
		if missing(sasflag_dq_excluded) and upcase(strip(c_ind_fv_indicator))="Y" 	then sasflag_dq_excluded = 3;
		if missing(sasflag_dq_excluded) and index(c_final_rating,"8") > 0  			then sasflag_dq_excluded = 4.1;
		if missing(sasflag_dq_excluded) and index(car_rating,"8") > 0 				then sasflag_dq_excluded = 4.2;
		if missing(sasflag_dq_excluded) and upcase(strip(c_ind_incompleted_fs))="Y"	then sasflag_dq_excluded = 5;

	run;
	proc sort data=&out. out=&keytbl.(keep=n_raid n_archive_id sasflag_dq_excluded) nodupkey; 
		by n_raid n_archive_id;
		where not missing(sasflag_dq_excluded);
	run;
%mend;
%gen_MM_Exclusion(stdte=&st_BackMth.,	rmp=RMP2BIOMM);
%gen_MM_Exclusion(stdte=&st_RptMth.,	rmp=RMP2BIOMM);