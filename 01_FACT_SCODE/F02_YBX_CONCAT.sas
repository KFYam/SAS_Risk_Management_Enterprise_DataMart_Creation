/* ******************************************************************************************** */
/* STEP 0 - format classified loans into a proper sequence  */
/* ******************************************************************************************** */
data work.classifiedloan;
	infmtn	='cl_i'; type_in='I';
	fmtn	='cl_f'; type_f='N';
	cl='P1M'; seq= 1.0; output;  /* Good (Manual) */
	cl='P1A'; seq= 1.1; output;  /* Good (Auto) */
	cl='P2M'; seq= 2.0; output;  /* Satisfactory (Manual) */
	cl='P2A'; seq= 2.1; output;  /* Satisfactory (Auto) */
	cl='P3M'; seq= 3.0; output;  /* Acceptable (Manual) */
	cl='P3A'; seq= 3.1; output;  /* Acceptable (Auto) */
	cl='S1M'; seq= 4.0; output;  /* Special Mention 1 (Manual) */
	cl='S1A'; seq= 4.1; output;  /* Special Mention 1 (Auto) */
	cl='S2M'; seq= 5.0; output;  /* Special Mention 2 (Manual) */
	cl='S2A'; seq= 5.1; output;  /* Special Mention 2 (Auto) */
	cl='SSM'; seq= 6.0; output;  /* Sub-standard (Manual) */
	cl='SSA'; seq= 6.1; output;  /* Sub-standard (Auto) */
	cl='DFM'; seq= 7.0; output;  /* Doubtful (Manual) */
	cl='DFA'; seq= 7.1; output;  /* Doubtful (Auto) */
	cl='LSM'; seq= 8.0; output;  /* Loss (Manual) */
	cl='LSA'; seq= 8.1; output;  /* Loss (Auto) */
run;
proc format cntlin=work.classifiedloan(rename=(cl=start seq=label infmtn=fmtname type_in=type));
proc format cntlin=work.classifiedloan(rename=(seq=start cl=label fmtn=fmtname type_f=type));
/* For checking purpose only: data aa;test="P1M";	a=input(test,cl_i.);b=put(1,cl_f.); run; */
/* ******************************************************************************************** */

%macro init_genYBX(ybx=);
%let st_frm=200912;
%let st_end=201602;
%let range = %SYSFUNC(intck(MONTH,%SYSFUNC(inputn(&st_frm.01,yymmdd8.)),%SYSFUNC(inputn(&st_end.01,yymmdd8.))));
%do i=0 %to &range.;
	%let dte 		= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&st_frm.01,yymmdd8.)),&i. ,END));
	%let st_RptMth	= %SYSFUNC(putn(&dte.,yymmn6.));
	%put testing  &st_RptMth.;
	%if %sysfunc(exist(raw_&ybx..&ybx.mast_&st_RptMth.)) and not %sysfunc(exist(fact.&ybx._cinlevel_&st_RptMth.)) %then %do;
		%genYBX(ybx=&ybx.);
	%end;
%end;
%mend;
/* 
%init_genYBX(ybx=ybc); 
%init_genYBX(ybx=ybd); 
*/

%macro genYBX(ybx=);
	proc sql noprint;
		create table fact.&ybx._cinlevel_&st_RptMth. as
		select  d_Inc_Acdate, n_CIF_NO, c_ID,   
			sum(n_HKD_EQUIVALENT) as hkd_equivalent,
			put(max(input(c_ccls,cl_i.)), cl_f.) as c_ccls, 
			max(n_OVERDUE_PRD_FRM2) as n_overdue_prd_frm2 
		from raw_&ybx..&ybx.mast_&st_RptMth.
		group by d_Inc_Acdate, n_CIF_NO, c_ID    
		order by d_Inc_Acdate, n_CIF_NO, c_ID  
		;
	quit;
%mend;

%macro gen_viewcombined(ybx=);
	%if %sysfunc(exist(fact.&ybx._cinlevel_allhist_&st_RptMth.)) %then %do;
		%put ---- NCB LOG ---- : !! fact.&ybx._cinlevel_allhist_&st_RptMth. is already existed.;
	%end;
	%else %do;
		data _null_;
			file "&dir_tmp./ybx_cat&st_TS..dat" lrecl=65535;
			put "data fact.&ybx._cinlevel_allhist_&st_RptMth./view=fact.&ybx._cinlevel_allhist_&st_RptMth.;";
			put "set";
		run;
		data _null_;
			file "&dir_tmp./ybx_cat&st_TS..dat" mod;
			set sashelp.vtable;
			where 	libname="FACT" and 
					substr(memname,1,12)=upcase("&ybx._cinlevel") and
					compress(substr(memname,14,6),"0123456789")="" and
					intnx("month",input(catt(substr(memname,14,6),"01"), yymmdd8.),0,"end") <= &dt_RptMth.;
			tmp=strip(libname)||"."||strip(memname);
			put tmp;
		run;
		data _null_;
			file "&dir_tmp./ybx_cat&st_TS..dat" mod;
			put "; run;";
		run;
		%include "&dir_tmp./ybx_cat&st_TS..dat" /source2;
	%end;
%mend;

%macro gen_YBX_FACT(ybx=);
	%if %sysfunc(exist(fact.&ybx._cinlevel_&st_RptMth.)) %then %do;
		%put ---- NCB LOG ---- : !! fact.&ybx._cinlevel_&st_RptMth. is already existed.;
	%end;
	%else %do;
		%if not %sysfunc(exist(raw_&ybx..&ybx.mast_&st_RptMth.)) %then %do;	
			%put ---- NCB LOG ---- : !! raw_&ybx..&ybx.mast_&st_RptMth. does not exist.;	
		%end;
		%else %do;
			%genYBX(ybx=&ybx.);
			%gen_viewcombined(ybx=&ybx.);
		%end;
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=YBX_&st_RptMth._CONCAT_&st_cltinfo.);
%gen_YBX_FACT(ybx=YBC);
%gen_YBX_FACT(ybx=YBD);
%exportlog(ind=STOP);
