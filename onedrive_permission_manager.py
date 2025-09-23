# 使用Microsoft Graph API操作账号下的文件夹权限，根据配置赋权或取消赋权给指定账号，并询问是否进行递归操作以检查是否有文件或子文件夹权限不正确

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
from azure.identity import DeviceCodeCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.drive_recipient import DriveRecipient
from msgraph.generated.drives.item.items.item.invite.invite_post_request_body import InvitePostRequestBody

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 目标账号的邮箱
RECIPIENT_EMAIL = "aaaaaaa@xxxxxxx.onmicrosoft.com"
# 分享控制: 'read'、'write' 执行分享；'none' 执行取消分享
SHARE_PERMISSION = "read"
# 要分享的文件夹路径 (相对于 OneDrive 根目录)
FOLDER_PATH = "/新建文件夹"
id2Name = dict()

async def item_permissions_handler(graph_client: GraphServiceClient, drive_id: str, item_id: str):
    """
    先查询item的权限，然后根据SHARE_PERMISSION决定是赋权还是取消赋权还是不操作，仅处理传入的item_id
    """
    print(f"开始处理项目：{id2Name.get(item_id, item_id)} 的权限")
    try:
        # 拉取该项的所有权限（包含分页）
        all_permissions = []
        result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.get()
        while True:
            if result and getattr(result, "value", None):
                all_permissions.extend(result.value or [])
            next_link = getattr(result, "odata_next_link", None) if result else None
            if not next_link:
                break
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.with_url(next_link).get()

        def identity_matches_email(identity, email):
            if not identity or not email:
                return False
            email_l = email.lower()
            user = getattr(identity, "user", None)
            if user:
                # IdentitySet.user.email
                if getattr(user, "email", None):
                    return str(user.email).lower() == email_l
                # IdentitySet.user.additional_data['email']
                if getattr(user, "additional_data", None) and user.additional_data.get('email', None):
                    return str(user.additional_data['email']).lower() == email_l
            # 某些模型可能直接有 email 字段
            if getattr(identity, "email", None):
                return str(identity.email).lower() == email_l
            return False

        def permission_for_email(perm, email):
            # 单个主体
            if identity_matches_email(getattr(perm, "granted_to_v2", None), email):
                return True
            if identity_matches_email(getattr(perm, "granted_to", None), email):
                return True
            # 多个主体
            for ident in getattr(perm, "granted_to_identities_v2", []) or []:
                if identity_matches_email(ident, email):
                    return True
            for ident in getattr(perm, "granted_to_identities", []) or []:
                if identity_matches_email(ident, email):
                    return True
            return False

        target_perms = [p for p in all_permissions if permission_for_email(p, RECIPIENT_EMAIL)]
        has_read = any("read" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)
        has_write = any("write" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)

        desired = (SHARE_PERMISSION or "").lower().strip()

        # 取消分享
        if desired == "none":
            if not target_perms:
                print("无可取消的权限。")
                return
            for p in target_perms:
                if getattr(p, "id", None):
                    await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.by_permission_id(p.id).delete()
            print(f"已取消 {RECIPIENT_EMAIL} 的权限。")
            return

        # 赋予/调整分享
        if desired in ("read", "write"):
            # 已满足目标权限则不操作
            if desired == "write" and has_write:
                print(f"{RECIPIENT_EMAIL} 已具有写入权限，无需更改。")
                return
            if desired == "read" and has_read and not has_write:
                print(f"{RECIPIENT_EMAIL} 已具有读取权限，无需更改。")
                return

            # 需要变更：先删除原有权限，再重新邀请
            for p in target_perms:
                if getattr(p, "id", None):
                    await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).permissions.by_permission_id(p.id).delete()

            body = InvitePostRequestBody(
                recipients=[DriveRecipient(email=RECIPIENT_EMAIL)],
                require_sign_in=True,
                send_invitation=False,
                roles=[desired],
            )
            await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).invite.post(body)
            print(f"已为 {RECIPIENT_EMAIL} 赋予 {desired} 权限。")
            return

        print(f"未知的 SHARE_PERMISSION: {SHARE_PERMISSION}，不执行操作。")
    except Exception as e:
        print(f"处理权限时出错: {e}")

async def manage_permissions(graph_client: GraphServiceClient, drive_id: str, item_id: str):
    """
    根据配置的 SHARE_PERMISSION 来赋权或取消赋权给指定账号。
    """
    # 先处理传入的 item
    # 处理完当前项后，询问用户是否递归处理子项
    # 使用list记录待处理的项，将递归操作转为循环操作
    global id2Name
    # 处理当前项
    await item_permissions_handler(graph_client, drive_id, item_id)

    # 询问是否递归
    choice = input("是否递归处理子项? (y/N): ").strip().lower()
    if choice != "y":
        return

    # 使用队列进行迭代遍历
    queue = [item_id]
    visited = set()

    while queue:
        parent_id = queue.pop(0)
        if not parent_id or parent_id in visited:
            continue
        visited.add(parent_id)

        # 获取父项详情（用于判断是否为文件夹）
        try:
            parent_item = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(parent_id).get()
            if parent_item and getattr(parent_item, "id", None) and getattr(parent_item, "name", None):
                id2Name[parent_item.id] = parent_item.name
            # 仅文件夹才有 children
            if not parent_item or not getattr(parent_item, "folder", None):
                continue
        except Exception as e:
            print(f"获取项目详情失败: {e}")
            continue

        # 分页遍历子项
        try:
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(parent_id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        cid = getattr(child, "id", None)
                        if not cid:
                            continue
                        if getattr(child, "name", None):
                            id2Name[cid] = child.name
                        # 处理子项权限
                        await item_permissions_handler(graph_client, drive_id, cid)
                        # 入队以继续向下遍历
                        queue.append(cid)

                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(parent_id).children.with_url(next_link).get()
        except Exception as e:
            print(f"枚举子项失败: {e}")
            continue

async def get_drive_item_by_path(graph_client: GraphServiceClient, drive_id: str, path: str):
    """
    通过路径逐段遍历 children 来获取 DriveItem；避免依赖 SDK 中可能缺失的 get_by_path/item_with_path。
    返回找到的 DriveItem，否则返回 None。
    """
    try:
        global id2Name
        normalized = (path or "").strip().strip("/")
        # 获取根 DriveItem 以拿到 root 的 item_id
        root_item = await graph_client.drives.by_drive_id(drive_id).root.get()
        if not root_item or not getattr(root_item, "id", None):
            return None

        # 空路径表示根
        if not normalized:
            return root_item

        current_id = root_item.id
        if not current_id:
            return None
        segments = [seg for seg in normalized.split("/") if seg]

        for seg in segments:
            found = None
            # 获取当前节点的所有子项（处理分页）
            if not current_id:
                return None
            result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.get()
            while True:
                if result and getattr(result, "value", None):
                    for child in (result.value or []):
                        if getattr(child, "name", None) == seg:
                            found = child
                            break
                if found is not None:
                    break
                next_link = getattr(result, "odata_next_link", None) if result else None
                if not next_link:
                    break
                result = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.with_url(next_link).get()

            if found is None or not getattr(found, "id", None):
                return None
            current_id = found.id
            id2Name[found.id] = found.name

        # 返回最终节点的完整详情
        if not current_id:
            return None
        final_item = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).get()
        return final_item
    except Exception:
        # 让调用方决定如何提示错误，这里返回 None
        return None

async def main():
    """
    主函数，用于认证并启动文件权限管理流程。
    """
    # 定义权限范围
    scopes = ["https://graph.microsoft.com/.default"]
    
    try:
        credential = DeviceCodeCredential(client_id=CLIENT_ID)
        graph_client = GraphServiceClient(credentials=credential, scopes=scopes)

        # 获取用户信息，从而找到 Drive ID
        drive = await graph_client.me.drive.get()
        if not drive or not drive.id:
            print("无法获取用户的 Drive 信息。请确保账户有 OneDrive for Business。")
            return
        
        drive_id = drive.id
        print(f"成功获取 Drive ID: {drive_id}")

        # 根据路径获取文件夹的 DriveItem
        print(f"正在查找文件夹: '{FOLDER_PATH}'")
        try:
            # 通过逐段遍历 children 的方式来解析路径
            drive.root
            target_folder = await get_drive_item_by_path(graph_client, drive_id, FOLDER_PATH)
            if not target_folder or not target_folder.id:
                print(f"找不到指定的文件夹: '{FOLDER_PATH}'")
                return
            
            print(f"找到文件夹 ID: {target_folder.id}")
            
            # 处理文件夹权限
            await manage_permissions(graph_client, drive_id, target_folder.id)
            print("\n文件夹权限处理完成。")

        except Exception as e:
            print(f"通过路径 '{FOLDER_PATH}' 查找文件夹时出错: {e}")
            print("请检查路径是否正确，以及应用是否具有足够的权限 (例如 Files.ReadWrite.All)。")

    except Exception as e:
        print(f"发生错误: {e}")
        if "AADSTS700016" in str(e):
            print("认证错误: 应用标识符(CLIENT_ID)可能不正确或未在目标租户中正确配置。")
        elif "AADSTS900561" in str(e):
             print("认证错误: 设备代码认证流程未完成或已超时。")


if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行认证。请在浏览器中打开认证页面并输入以下代码。")
    asyncio.run(main())
