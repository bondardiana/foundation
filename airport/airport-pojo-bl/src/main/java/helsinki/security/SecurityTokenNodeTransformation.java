package helsinki.security;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.concat;
import static ua.com.fielden.platform.types.tuples.T2.t2;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import helsinki.security.tokens.OtherModuleToken;
import helsinki.security.tokens.UsersAndPersonnelModuleToken;
import ua.com.fielden.platform.security.ISecurityToken;
import ua.com.fielden.platform.security.provider.ISecurityTokenNodeTransformation;
import ua.com.fielden.platform.security.provider.SecurityTokenNode;
import ua.com.fielden.platform.security.tokens.attachment.AttachmentDownload_CanExecute_Token;
import ua.com.fielden.platform.security.tokens.attachment.Attachment_CanDelete_Token;
import ua.com.fielden.platform.security.tokens.attachment.Attachment_CanSave_Token;
import ua.com.fielden.platform.security.tokens.user.UserRoleTokensUpdater_CanExecute_Token;
import ua.com.fielden.platform.security.tokens.user.UserRole_CanDelete_Token;
import ua.com.fielden.platform.security.tokens.user.UserRole_CanSave_Token;
import ua.com.fielden.platform.security.tokens.user.UserRolesUpdater_CanExecute_Token;
import ua.com.fielden.platform.security.tokens.user.User_CanDelete_Token;
import ua.com.fielden.platform.security.tokens.user.User_CanSave_Token;
import ua.com.fielden.platform.utils.CollectionUtil;

/**
 * Transforms a tree of security tokens to another that incorporates TG platfrom specific tokens under the right top-level application modules.
 *
 * @author Generated
 */
public class SecurityTokenNodeTransformation implements ISecurityTokenNodeTransformation {

    private static final Map<Class<? extends ISecurityToken>, Set<Class<? extends ISecurityToken>>> tokensToRestructure = CollectionUtil.mapOf(
            t2(OtherModuleToken.class, CollectionUtil.setOf(Attachment_CanSave_Token.class, Attachment_CanDelete_Token.class, AttachmentDownload_CanExecute_Token.class)),
            t2(UsersAndPersonnelModuleToken.class, CollectionUtil.setOf(User_CanSave_Token.class, User_CanDelete_Token.class, UserRole_CanSave_Token.class, UserRole_CanDelete_Token.class, UserRolesUpdater_CanExecute_Token.class, UserRoleTokensUpdater_CanExecute_Token.class)));

    @Override
    public SortedSet<SecurityTokenNode> transform(final SortedSet<SecurityTokenNode> topLevelSecurityTokenNodes) {
        return topLevelSecurityTokenNodes.stream()
                .filter(node -> !tokensToRestructure.values().stream().anyMatch(set -> set.contains(node.getToken())))
                .map(node -> {
                    final SecurityTokenNode newNode = new SecurityTokenNode(node.getToken());
                    concat(tokensToRestructure.getOrDefault(node.getToken(), Collections.emptySet()).stream(),
                           node.getSubTokenNodes().stream().map(SecurityTokenNode::getToken))
                    .forEach(token -> new SecurityTokenNode(token, newNode));
                    return newNode;
                })
                .collect(toCollection(TreeSet::new));
    }

}