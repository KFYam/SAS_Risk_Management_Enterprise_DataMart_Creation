%let st_TS			= %get_TSstring();
%let st_cltinfo		= U&_METAUSER._%sysfunc(dequote(&_CLIENTMACHINE.));
%let st_tblpwd		= P%substr(&_METAUSER.,2,4)%substr(%sysfunc(dequote(&_CLIENTMACHINE.)),8,3);

%let dt_RptMth		= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&st_RptMth.01,yymmdd8.)),0,END));
%let dt_BackMth		= %SYSFUNC(intnx(MONTH,&dt_RptMth.,-12,END));	/* This is Backend Month, i.e. 1-year before Reporting Month */
%let dt_BBegMth		= %SYSFUNC(intnx(MONTH,&dt_BackMth.,1,BEGIN));	/* This is Start Month of Backend Observation Window, i.e. Next Month of Backend Month */
%let dt_BEndMth		= %SYSFUNC(intnx(MONTH,&dt_BackMth.,12,END));	/* This is End Month of Backend Observation Window, i.e. Next 12 Months of Backend Month */

%let st_RptYMD		= %SYSFUNC(putn(&dt_RptMth.,yymmddn8.));
%let st_RptYMD6		= %SYSFUNC(putn(&dt_RptMth.,yymmddn6.));
%let st_RptYYMM		= %SYSFUNC(putn(&dt_RptMth.,yymmn4.));

%let st_BackMth		= %SYSFUNC(putn(&dt_BackMth.,yymmn6.));
%let st_BackYMD		= %SYSFUNC(putn(&dt_BackMth.,yymmddn8.));
%let st_BackYMD6	= %SYSFUNC(putn(&dt_BackMth.,yymmddn6.));
%let st_BackYYMM	= %SYSFUNC(putn(&dt_BackMth.,yymmn4.));

%let st_BBegMth		= %SYSFUNC(putn(&dt_BBegMth.,yymmn6.));
%let st_BBegYMD		= %SYSFUNC(putn(&dt_BBegMth.,yymmddn8.));
%let st_BBegYMD6	= %SYSFUNC(putn(&dt_BBegMth.,yymmddn6.));
%let st_BBegYYMM	= %SYSFUNC(putn(&dt_BBegMth.,yymmn4.));

%let st_BEndMth		= %SYSFUNC(putn(&dt_BEndMth.,yymmn6.));
%let st_BEndYMD		= %SYSFUNC(putn(&dt_BEndMth.,yymmddn8.));
%let st_BEndYMD6	= %SYSFUNC(putn(&dt_BEndMth.,yymmddn6.));
%let st_BEndYYMM	= %SYSFUNC(putn(&dt_BEndMth.,yymmn4.));

%put ---- NXX LOG ---- : Macro Variable st_RptMth is &st_RptMth.;
%put ---- NXX LOG ---- : Macro Variable st_RptYMD is &st_RptYMD.;
%put ---- NXX LOG ---- : Macro Variable st_TS is &st_TS.;
%put ---- NXX LOG ---- : Macro Variable st_cltinfo is &st_cltinfo.;
%put ---- NXX LOG ---- : Macro Variable st_BackYMD is &st_BackYMD.;
%put ---- NXX LOG ---- : Macro Variable st_BBegYMD is &st_BBegYMD.;
%put ---- NXX LOG ---- : Macro Variable st_BEndYMD is &st_BEndYMD.;

