package auth

import "regexp"

// User represents an AMQP user with credentials and per-vhost permissions.
type User struct {
	Name        string                `json:"name"`
	Password    Password              `json:"password_hash"`
	Tags        []string              `json:"tags"`
	Permissions map[string]Permission `json:"permissions"` // vhost name -> permission
}

// Permission defines regex patterns controlling access to resources within a vhost.
type Permission struct {
	Configure string `json:"configure"` // regex pattern
	Read      string `json:"read"`      // regex pattern
	Write     string `json:"write"`     // regex pattern
}

// HasTag reports whether the user has the given tag.
func (u *User) HasTag(tag string) bool {
	for _, ut := range u.Tags {
		if ut == tag {
			return true
		}
	}

	return false
}

// CheckPermission reports whether the user may perform action on resource in vhost.
// Action must be "configure", "read", or "write".
func (u *User) CheckPermission(vhost, resource, action string) bool {
	perm, ok := u.Permissions[vhost]
	if !ok {
		return false
	}

	var pattern string

	switch action {
	case "configure":
		pattern = perm.Configure
	case "read":
		pattern = perm.Read
	case "write":
		pattern = perm.Write
	default:
		return false
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}

	return re.MatchString(resource)
}

// SetPermission sets the permission for the given vhost, replacing any existing permission.
func (u *User) SetPermission(vhost string, perm Permission) {
	if u.Permissions == nil {
		u.Permissions = make(map[string]Permission)
	}

	u.Permissions[vhost] = perm
}
