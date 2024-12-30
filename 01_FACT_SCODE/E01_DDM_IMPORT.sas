/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

/* As the prefix of monthly DDM files is slowly changing, the suffix of monthly DDM files is also	*/
/* not exactly the same as the report generation date, in other words, it is usually few dates next */
/* to end of month. The program below targets to find the proper input file by comparing between 	*/
/* the generation date and infiles for getting the smallest absolute date gap with 5 days cap.		*/
/* Where 5 days caps is judgementally assigned and subject to review regularly. */

%let max_dategap=5;

%macro gen_DDM();
	%if %SYSFUNC(exist(raw_ddm.rm1ddmon_&st_RptMth.)) = 1 %then %do;
		%put ---- NCB LOG ---- : !! raw_ddm.rm1ddmon_&st_RptMth. is already existed.;
	%end;
	%else %do;
		x "cd &dir_ddm.";
		x "ls -al > filelist.txt";
		data ddm_fileinfo;
			infile "&dir_ddm./filelist.txt" 
			DLM='7F'x MISSOVER DSD ;
			INPUT  text   : $CHAR80.;
			text1 = reverse(trim(text));
			if upcase(substr(text1,1,4)) = reverse(".CSV") then do;
				index=index(text1," ");
				csvname=reverse(substr(text1,1,index-1));
				csvname_prefix=tranwrd(csvname,"_"||scan(csvname,-1,'_'),"");
				csvname_suffix=tranwrd(scan(upcase(csvname),-1,'_'),".CSV", "");

				d_srcfile_date = input(csvname_suffix,yymmdd8.);
				d_mthend =intnx("MONTH",d_srcfile_date ,0,"END");
				n_daydiff = intck("DAY",d_srcfile_date, d_mthend);

				if -&max_dategap. <= n_daydiff <= &max_dategap. then 
					d_rptmth = d_mthend;
				else if -&max_dategap. < n_daydiff  then 
					d_rptmth = intnx("MONTH",d_srcfile_date ,-1,"END");
				else if n_daydiff > &max_dategap. then 
					d_rptmth = intnx("MONTH",d_srcfile_date ,-1,"END");

				s_rptmth=put(d_rptmth, yymmn6.);
				format d_srcfile_date d_rptmth  d_mthend yymmdd10.;
				output;
			end;
		run;
		/* Modified on 19 Jan 2017 */
		proc sort data=ddm_fileinfo(where=(d_rptmth =&dt_RptMth.)) out=ddm_fileinfo_v1;
			by n_daydiff;
		run;
		data _null_;
			set ddm_fileinfo_v1(obs=1);
			by n_daydiff;
			call symput("csvname",strip(csvname));
			call symput("ddm_rptmth",strip(s_rptmth));
		run;
		%if &csvname. ne  %then %do;
			%import_InputMaster(sheet=MetaInfo_DDM, out=raw_ddm.meta_ddm_&st_RptMth.);
			%gen_SRC_IN_CHAR(orig_src=&dir_ddm./&csvname., orig_delim=",", sas_tblname=stg_ddm.rm1ddmon_&st_RptMth., realtime_meta=work.rtmeta_rm1ddmon_&st_RptMth., encode=); 
			%trim_SRC_FIELDNAME(proposed_meta=raw_ddm.meta_ddm_&st_RptMth., realtime_meta=work.rtmeta_rm1ddmon_&st_RptMth., final_meta=stg_ddm.rtmeta_rm1ddmon_&st_RptMth.);
			%update_META2SRC(metatbl=stg_ddm.rtmeta_rm1ddmon_&st_RptMth., srctbl=stg_ddm.rm1ddmon_&st_RptMth., tartbl=raw_ddm.rm1ddmon_&st_RptMth.);
		%end;
		%else %do;
			%put ---- NCB LOG ---- : !! suitable file at the month of &st_RptMth. does not exist.;
		%end;
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=DDM_&st_RptMth._LOAD_&st_cltinfo.);
%import_Format(src=DDM);
%gen_DDM;
%exportlog(ind=STOP);
