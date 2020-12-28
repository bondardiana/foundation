package helsinki.security.tokens;

import helsinki.config.Modules;
import ua.com.fielden.platform.security.ISecurityToken;

/**
 * Top level security token for all security tokens that belong to module {@link Modules#OTHER};
 *
 * @author Generated
 */

public class OtherModuleToken implements ISecurityToken {
    public static final String TITLE = Modules.OTHER.title;
    public static final String DESC = Modules.OTHER.desc;
}
