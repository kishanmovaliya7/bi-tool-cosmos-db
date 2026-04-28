const jwt = require('jsonwebtoken');
const { SQLquery } = require('../services/dbConnect');

/**
 * Generate JWT token with standard payload and expiry
 * @param {Object} payload - Token payload {userId, accountId, email, roleId}
 * @param {string} expiresIn - Token expiry (default: '1d')
 * @returns {string} JWT token
 */
const generateAuthToken = (payload, expiresIn = '1d') => {
  return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn });
};

/**
 * Fetch complete user profile with account and team information
 * @param {number} userId - User ID
 * @returns {Object} User profile object
 */
const fetchUserProfile = async (userId) => {
  // First, get all user accounts to determine default account
  const userAccounts = await SQLquery(
    `SELECT 
        ua.account_id,
        ua.account_type,
        a.company as account_name
     FROM bi.user_account_map ua
     JOIN bi.accounts a ON a.id = ua.account_id
     WHERE ua.user_id = @param0
       AND ua.status_id = 2
     ORDER BY ua.created_at ASC;`,
    [userId]
  );

  // Determine default account based on business rules
  let defaultAccountId = null;
  if (userAccounts && userAccounts.length > 0) {
    // Priority 1: account_type = 'org'
    const orgAccount = userAccounts.find(account => account.account_type === 'org');
    if (orgAccount) {
      defaultAccountId = orgAccount.account_id;
    } else {
      // Priority 2: first account (account_type = 'self')
      defaultAccountId = userAccounts[0].account_id;
    }
  }

  // Now fetch the complete user profile with all user accounts
  const userInfo = await SQLquery(
    `SELECT 
        u.id AS user_id,
        u.email,
        u.first_name,
        u.last_name,
        u.username,
        u.widgets_expiry_after_days,
        u.logo_url as logo,
        @param1 as default_account,
        a.id AS account_id,
        a.status_id,
        a.company as account_name,
        a.city,
        a.state,
        a.phone,
        ua.role_id,
        ab.tertiary_color,
        ab.secondary_color,
        ab.primary_color,
        ab.custom_domain,
        ab.logo_url,
        r.role_name
     FROM bi.users u
     JOIN bi.user_account_map ua ON ua.user_id = u.id
     JOIN bi.accounts a ON a.id = ua.account_id
     left join bi.account_branding ab on ab.account_id = a.id
     JOIN bi.roles r ON r.id = ua.role_id
     WHERE u.id = @param0
       AND u.status_id = 2
       AND ua.status_id = 2;`,
    [userId, defaultAccountId]
  );

  // Convert to desired structure
  const userProfile = {
    user_id: null,
    email: null,
    first_name: null,
    last_name: null,
    username: null,
    default_account: null,
    accountInfo: []
  };

  if (userInfo.length > 0) {
    const firstRow = userInfo[0];
    
    // Set user basic info (same across all rows)
    userProfile.user_id = firstRow.user_id;
    userProfile.email = firstRow.email;
    userProfile.first_name = firstRow.first_name;
    userProfile.last_name = firstRow.last_name;
    userProfile.username = firstRow.username;
    userProfile.widgets_expiry_after_days = firstRow.widgets_expiry_after_days;
    userProfile.logo = firstRow.logo;
    userProfile.default_account = firstRow.default_account;
    
    // Set account info (same across all rows)
    userProfile.accountInfo = userInfo.map(row => ({
      account_id: row.account_id,
      status_id: row.status_id,
      account_name: row.account_name,
      city: row.city,
      state: row.state,
      phone: row.phone,
      role_id: row.role_id,
      role_name: row.role_name,
      tertiary_color: row.tertiary_color,
      secondary_color: row.secondary_color,
      primary_color: row.primary_color,
      custom_domain: row.custom_domain,
      logo_url: row.logo_url
    }));
  }

  return userProfile;
};


module.exports = {
  generateAuthToken,
  fetchUserProfile
};