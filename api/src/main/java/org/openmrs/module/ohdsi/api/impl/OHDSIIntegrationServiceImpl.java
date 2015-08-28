/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.ohdsi.api.impl;

import org.openmrs.api.impl.BaseOpenmrsService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.ohdsi.api.OHDSIIntegrationService;
import org.openmrs.module.ohdsi.api.db.OHDSIIntegrationDAO;

/**
 * It is a default implementation of {@link OHDSIIntegrationService}.
 */
public class OHDSIIntegrationServiceImpl extends BaseOpenmrsService implements OHDSIIntegrationService {
	
	protected final Log log = LogFactory.getLog(this.getClass());
	
	private OHDSIIntegrationDAO dao;
	
	/**
     * @param dao the dao to set
     */
    public void setDao(OHDSIIntegrationDAO dao) {
	    this.dao = dao;
    }
    
    /**
     * @return the dao
     */
    public OHDSIIntegrationDAO getDao() {
	    return dao;
    }
}