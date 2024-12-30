/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_YBDData(); 
	%if %SYSFUNC(exist(raw_ybd.YBDMAST_&st_RptMth.)) = 1 %then %do;
		%put ---- NCB LOG ---- : !! raw_ybd.YBDMAST_&st_RptMth. is already existed.;
	%end;
	%else %do;
		x "cd &dir_ybd./csv";
		x "ls -al > filelist.txt";
		data ybd_fileinfo;
			infile "&dir_ybd./csv/filelist.txt" 
			DLM='7F'x MISSOVER DSD ;
			INPUT  text   : $CHAR80.;
			text1 = reverse(trim(text));
			if substr(text1,1,4) = reverse(".csv") then do;
				index=index(text1," ");
				csvname=reverse(substr(text1,1,index-1));
				separator=index(csvname,"_");
				csvname_prefix=substr(csvname,1,separator-1);
				csvname_suffix=tranwrd(substr(csvname,separator+1,length(csvname)),".csv", "");

				if csvname_prefix in ("T160225C" "T160226C") then do;
					csvname_prefix_n="YBDMAST"; 
					FLAG=1;
				end;
				else do;
					csvname_prefix_n=compress(csvname_prefix);
				end;
				if length(compress(csvname_suffix)) ne  6 then FLAG=1;
				csvname_suffix_n=substr(csvname_suffix,1,6);

				newname=compress(csvname_prefix_n||"_"||csvname_suffix_n||".csv");
			end;
			if not missing(newname) then output;
		run;
		proc sort data=ybd_fileinfo(where=(FLAG=1)) out=for_rename nodupkey; by newname; run;
		data _null_;
			file "&dir_tmp./ybd1&st_TS..dat" lrecl=65535;
			set for_rename;
			by newname;
			t1= '&dir_ybd./csv/'||compress(csvname);
			t2= '&dir_ybd./csv/'||compress(newname);
			put 'x "mv ' t1 ' ' t2 '";';
		run;
		/* Rename the T160225C T160226C prefix into YBDMAST prefix */
		%include "&dir_tmp./ybd1&st_TS..dat" /source2; 

		%import_InputMaster(sheet=MetaInfo_YBDMAST, out=raw_ybd.meta_ybdmast_&st_RptMth.);
		%gen_SRC_IN_CHAR(orig_src=&dir_ybd./csv/YBDMAST_&st_RptMth..csv, orig_delim=",", sas_tblname=stg_ybd.ybdmast_&st_RptMth., realtime_meta=work.rtmeta_ybdmast_&st_RptMth., encode=); 
		%trim_SRC_FIELDNAME(proposed_meta=raw_ybd.meta_ybdmast_&st_RptMth., realtime_meta=work.rtmeta_ybdmast_&st_RptMth., final_meta=stg_ybd.rtmeta_ybdmast_&st_RptMth.);
		%update_META2SRC(metatbl=stg_ybd.rtmeta_ybdmast_&st_RptMth., srctbl=stg_ybd.ybdmast_&st_RptMth., tartbl=raw_ybd.ybdmast_&st_RptMth.);
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=YBD_&st_RptMth._LOAD_&st_cltinfo.);
%import_Format(src=YBD);
%gen_ybdData();
%exportlog(ind=STOP);

