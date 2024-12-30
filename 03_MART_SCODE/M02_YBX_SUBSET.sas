%macro gen_YBX_CINID_Level(ybx=);
	%if %sysfunc(exist(mart_f.&YBX._cinlevel_after_&st_BackMth.)) %then %do;
		%put ---- NCB LOG ---- : !! mart_f.&YBX._cinlevel_after_&st_BackMth. is already existed.;
	%end;
	%else %do;
		proc sort data=fact.&YBX._cinlevel_allhist_&st_RptMth. out=work.&YBX._subset nodupkey;
			by n_CIF_NO c_id d_Inc_Acdate; 
			where &dt_BBegMth. <= d_Inc_Acdate<= &dt_BEndMth.;
		run;

		data mart_f.&YBX._cinlevel_after_&st_BackMth.(keep=n_CIF_NO c_id s_text_concat s_hkdeq_concat);
			length s_text $16 s_hkdeq $100 s_text_concat $400 s_hkdeq_concat $2000 ;
			retain s_text_concat s_hkdeq_concat ;
			set work.&YBX._subset ;
			by n_CIF_NO c_id; 
			s_text =strip(c_ccls)||"/"||put(n_overdue_prd_frm2,z2.)||" ("||put(d_Inc_Acdate,yymmd7.)||")";
			s_hkdeq =strip(put(hkd_equivalent,comma30.2))||" ("||put(d_Inc_Acdate,yymmd7.)||")";

			if first.c_id then do; call missing(s_text_concat);call missing(s_hkdeq_concat); end;
			s_text_concat=catx('0D0A'x,strip(s_text_concat),s_text);
			s_hkdeq_concat=catx('0D0A'x,strip(s_hkdeq_concat),s_hkdeq);
			if last.c_id then output;
		run;
	%end;	
%mend;

%exportlog(ind=START, path=&dir_log., name=YBC_&st_RptMth._CINID_MART_&st_cltinfo.);
%gen_YBX_CINID_Level(ybx=YBC);
%exportlog(ind=STOP);
%exportlog(ind=START, path=&dir_log., name=YBD_&st_RptMth._CINID_MART_&st_cltinfo.);
%gen_YBX_CINID_Level(ybx=YBD);
%exportlog(ind=STOP);
