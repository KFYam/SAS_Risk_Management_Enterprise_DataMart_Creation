/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_BPE(meta=); 

	%if &meta.=BPE 			 	 %then %do; %let txtfile=BPE&st_RptYYMM.; 				%let csv=csv; 				%let rawfile=bpe_&st_rptmth.;			%end;
	%if &meta.=BPE_RSME003 	 	 %then %do; %let txtfile=RSME003_043_&st_RptMth.;		%let csv=RSME003_csv;		%let rawfile=bpe_rsme003_&st_rptmth.;	%end;
	%if &meta.=BPE_RSME004 	 	 %then %do; %let txtfile=RSME004_043_&st_RptMth.;		%let csv=RSME004_csv;		%let rawfile=bpe_rsme004_&st_rptmth.;	%end;
	%if &meta.=BPE_RSMEPooling 	 %then %do; %let txtfile=RSME_Pooling_043_&st_RptMth.;	%let csv=RSMEPooling_csv;	%let rawfile=bpe_rsmepool_&st_rptmth.;	%end;

	%if &meta.=BPE_RSME_CollBasic  %then %do; %let txtfile=RSME_CollBasic_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_CollBasic_&st_rptmth.;%end;
	%if &meta.=BPE_RSME_CollAppo   %then %do; %let txtfile=RSME_CollAppo_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_CollAppo_&st_rptmth.; %end;
	%if &meta.=BPE_RSME_GuaBasic   %then %do; %let txtfile=RSME_GuaBasic_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_GuaBasic_&st_rptmth.;	%end;
	%if &meta.=BPE_RSME_CollGuaRel %then %do; %let txtfile=RSME_CollGuaRel_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_CollGuaRel_&st_rptmth.;%end;
	%if &meta.=BPE_RSME_GuaSupp    %then %do; %let txtfile=RSME_GuaSupp_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_GuaSupp_&st_rptmth.;	%end;
	%if &meta.=BPE_RSME_DegColl    %then %do; %let txtfile=RSME_DegColl_043_&st_RptMth.;	%let csv=RSMEColGua_csv;	%let rawfile=BPE_RSME_DegColl_&st_rptmth.;	%end;

	%if %SYSFUNC(exist(raw_bpe.&rawfile.)) = 1 %then %do;
		%put ---- NCB LOG ---- : !! raw_bpe.&rawfile. is already existed.;
	%end;
	%else %do;
		%if  %sysfunc(fileexist(&dir_bpe./&csv./&txtfile..csv)) %then %do;
			%let pmeta=meta_&rawfile.;
			%let rtmeta=rtmeta_&rawfile.;
			%import_InputMaster(sheet=MetaInfo_&meta., out=raw_bpe.meta_&meta._&st_RptMth.);

			%if &meta.=BPE_RSME_CollGuaRel %then %do;
				%gen_SRC_IN_CHAR(orig_src=&dir_bpe./&csv./&txtfile..csv, orig_delim=",", sas_tblname=stg_bpe.&rawfile., realtime_meta=work.rtmeta_BPE_RSME_CGR_&st_rptmth., encode=);
				%trim_SRC_FIELDNAME(proposed_meta=raw_bpe.meta_&meta._&st_RptMth., realtime_meta=work.rtmeta_BPE_RSME_CGR_&st_rptmth., final_meta=stg_bpe.rtmeta_BPE_RSME_CGR_&st_rptmth. );
				%update_META2SRC(metatbl=stg_bpe.rtmeta_BPE_RSME_CGR_&st_rptmth., srctbl=stg_bpe.&rawfile., tartbl=raw_bpe.&rawfile.);
			%end;
			%else %do;
				%gen_SRC_IN_CHAR(orig_src=&dir_bpe./&csv./&txtfile..csv, orig_delim=",", sas_tblname=stg_bpe.&rawfile., realtime_meta=work.rtmeta_&rawfile., encode=);
				%trim_SRC_FIELDNAME(proposed_meta=raw_bpe.meta_&meta._&st_RptMth., realtime_meta=work.rtmeta_&rawfile., final_meta=stg_bpe.rtmeta_&rawfile. );
				%update_META2SRC(metatbl=stg_bpe.rtmeta_&rawfile., srctbl=stg_bpe.&rawfile., tartbl=raw_bpe.&rawfile.);
			%end;
		%end;
		%else %do;
			%put ---- NCB LOG ---- : !! &dir_bpe./&csv./&txtfile..csv does not exist.;
		%end;
	%end;
%mend;

%import_Format(src=BPE);
%exportlog(ind=START, path=&dir_log., name=BPE_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME003_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME003);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME004_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME004);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSMEPOOL_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSMEPooling);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_COLLBASIC_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_CollBasic);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_COLLAPPO_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_CollAppo);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_GUABASIC_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_GuaBasic);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_COLLGUAREL_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_CollGuaRel);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_GUASUPP_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_GuaSupp);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=BPE_RSME_DEGCOLL_&st_RptMth._LOAD_&st_cltinfo.);
%gen_BPE(meta=BPE_RSME_DegColl);
%exportlog(ind=STOP); 

/*
%exportlog(ind=START, path=&dir_log., name=BPE_RSME004_&st_RptMth._LOAD);
options mprint;
%import_Format(src=BPE);
%gen_BPE(meta=BPE_RSME004);
%exportlog(ind=STOP); 
options mprint;
%import_Format(src=BPE);
%exportlog(ind=START, path=&dir_log., name=BPE_RSME003_&st_RptMth._LOAD);
%gen_BPE(meta=BPE_RSME003);
%exportlog(ind=STOP); 
/*
%gen_BPE(meta=BPE_RSMEPooling);
%exportlog(ind=STOP); 
*/
