__rethrow_casa_exceptions = True
context = h_init()
context.set_state('ProjectSummary', 'proposal_code', 'VLA Prop Code')
context.set_state('ProjectSummary', 'observatory', 'Karl G. Jansky Very Large Array')
context.set_state('ProjectSummary', 'telescope', 'EVLA')
context.set_state('ProjectSummary', 'piname', 'unknown')
context.set_state('ProjectSummary', 'proposal_title', 'unknown')
try:
    hifv_importdata(nocopy=True, vis=['VLASS1.1.sb34916486.eb35006898.58156.86241219907.ms'], session=['session_1'])
    hif_editimlist(parameter_file='parameter.list')
    hif_transformimagedata(datacolumn='corrected', modify_weights=False, clear_pointing=True)
    hif_makeimages(hm_masking='none', hm_cleaning='manual')
    hifv_pbcor(pipelinemode="automatic")
    hif_makermsimages(pipelinemode="automatic")
    hif_makecutoutimages(pipelinemode="automatic")
finally:
    h_save()
