# 使用Microsoft Graph API，从源OneDrive账户复制文件到目标OneDrive账户，并自动处理源文件的权限问题

# --- Azure AD 应用所需权限 (委托的权限) ---
# 1. Files.ReadWrite.All: 允许应用读取、创建、修改和删除所有用户的 OneDrive 文件。这是分享和遍历文件夹所必需的。
# 2. User.Read: 允许应用读取登录用户的基本个人资料。

import asyncio
from azure.identity import DeviceCodeCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem
from msgraph.generated.models.item_reference import ItemReference
from msgraph.generated.models.folder import Folder
from msgraph.generated.models.drive_recipient import DriveRecipient
from msgraph.generated.drives.item.items.item.copy.copy_post_request_body import CopyPostRequestBody
from msgraph.generated.drives.item.items.item.invite.invite_post_request_body import InvitePostRequestBody

# --- 配置信息 ---
# 在 Azure AD 中注册应用后获取，形如aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
# 源文件或文件夹路径
SOURCE_PATH = "/新建文件夹"
# 目标路径，如果目标路径不存在，脚本会自动创建
TARGET_PATH = "/"
id2Name = dict()

async def check_and_grant_permission(graph_client: GraphServiceClient, drive_id: str, item_id: str, target_mail: str):
    """
    检查指定用户是否对指定 DriveItem 拥有权限。如果没有，则赋予其读取权限。
    如果发起了赋权操作，则返回新的 Permission 对象，否则返回 None。
    """
    try:
        print(f"正在检查用户 '{target_mail}' 对项 ID '{id2Name.get(item_id, item_id)}' 的权限...")
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

        target_perms = [p for p in all_permissions if permission_for_email(p, target_mail)]
        has_read = any("read" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)
        has_write = any("write" in [str(r).lower() for r in (getattr(p, "roles", []) or [])] for p in target_perms)

        # 如果既没有读也没有写权限，则赋予读取权限
        if not has_read and not has_write:
            body = InvitePostRequestBody(
                recipients=[DriveRecipient(email=target_mail)],
                require_sign_in=True,
                send_invitation=False,
                roles=["read"]
            )
            print(f"用户 '{target_mail}' 没有权限，正在赋予读取权限...")
            perms = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).invite.post(body)
            if perms and getattr(perms, "value", None):
                print(f"成功赋予用户 '{target_mail}' 读取权限。")
                return perms
            else:
                print(f"赋予权限操作未返回预期结果，请手动检查。")
                raise Exception("赋权失败")
        else:
            print(f"用户 '{target_mail}' 已有{['读', '写'][has_write]}权限，跳过赋权。")

        return None
        
    except Exception as e:
        print(f"检查或赋权时发生错误: {e}")
        raise e
    

async def get_drive_item_by_path(graph_client: GraphServiceClient, drive_id: str, path: str, auto_create: bool = False):
    """
    通过路径逐段遍历 children 来获取 DriveItem。如果auto_create为True，则在路径不存在时自动创建文件夹。
    返回找到的 DriveItem，否则返回 None。
    """
    try:
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

            # 如果开启了自动创建且found为None，则创建文件夹
            if auto_create and found is None:
                request_body = DriveItem(name=seg, folder = Folder())
                found = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(current_id).children.post(request_body)

            if found is None or not getattr(found, "id", None):
                return None
            current_id = found.id
            id2Name[current_id] = found.name

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
    主函数
    """
    # 定义权限范围
    scopes = ["https://graph.microsoft.com/.default"]
    
    
    try:
        source_credential = DeviceCodeCredential(client_id=CLIENT_ID)
        source_client = GraphServiceClient(credentials=source_credential, scopes=scopes)
        source_account = await source_client.me.get()
        source_mail = getattr(source_account, 'mail')
        print(f"已登录源账户: {source_mail}")
        source_drive = await source_client.me.drive.get()
        if not source_drive or not source_drive.id:
            print("无法获取源账户的 Drive 信息。请确保源账户有 OneDrive for Business。")
            return

        target_credential = DeviceCodeCredential(client_id=CLIENT_ID)
        target_client = GraphServiceClient(credentials=target_credential, scopes=scopes)
        target_account = await target_client.me.get()
        target_mail = getattr(target_account, 'mail')
        print(f"已登录目标账户: {target_mail}")
        target_drive = await target_client.me.drive.get()
        if not target_drive or not target_drive.id:
            print("无法获取目标账户的 Drive 信息。请确保目标账户有 OneDrive for Business。")
            return

        source_drive_id = source_drive.id
        target_drive_id = target_drive.id
        print(f"成功获取 Drive ID：源账户 {source_drive_id}，目标账户 {target_drive_id}")

    except Exception as e:
        print(f"发生错误: {e}")
        if "AADSTS700016" in str(e):
            print("认证错误: 应用标识符(CLIENT_ID)可能不正确或未在目标租户中正确配置。")
        elif "AADSTS900561" in str(e):
             print("认证错误: 设备代码认证流程未完成或已超时。")
        return

    try:
        print(f"正在查找源路径 '{SOURCE_PATH}' ...")
        source_item = await get_drive_item_by_path(source_client, source_drive_id, SOURCE_PATH)
        if not source_item or not source_item.id:
            print(f"找不到源路径: '{SOURCE_PATH}'")
            return
        
        print(f'找到源项 ID: {source_item.id}')

        print(f"正在查找或创建目标路径 '{TARGET_PATH}' ...")
        target_item = await get_drive_item_by_path(target_client, target_drive_id, TARGET_PATH, auto_create=True)
        if not target_item or not target_item.id:
            print(f"无法找到或创建目标路径: '{TARGET_PATH}'")
            return
        
        print(f'找到目标项 ID: {target_item.id}')
    
    except Exception as e:
        print(f"查找路径时发生错误: {e}")
        return

    try:
        perms = await check_and_grant_permission(source_client, source_drive_id, source_item.id, target_mail)

        print(f"正在将源项 '{id2Name.get(source_item.id, source_item.id)}' 复制到目标路径 '{TARGET_PATH}' ...")
        copy_body = CopyPostRequestBody(
            name=source_item.name,
            parent_reference=ItemReference(drive_id=target_drive_id, id=target_item.id)
        )
        copied_item = await target_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(source_item.id).copy.post(copy_body)
        if copied_item:
            print(f"复制完成，新项 ID: {getattr(copied_item, 'id', '未知')}, 名称: {getattr(copied_item, 'name', '未知')}")
        else:
            print("复制操作未返回预期结果，请手动检查。")
        
        if perms and getattr(perms, "value", None):
            perm_list = perms.value or []
            print(f"正在撤销临时赋予的{len(perm_list)}个权限...")
            for p in perm_list:
                perm_id = getattr(p, "id", None)
                if perm_id:
                    await source_client.drives.by_drive_id(source_drive_id).items.by_drive_item_id(source_item.id).permissions.by_permission_id(perm_id).delete()
                    print(f"已撤销权限 ID: {perm_id}")

    except Exception as e:
        print(f"拷贝文件时发生错误: {e}")


if __name__ == "__main__":
    # 提示用户进行设备代码认证
    print("该脚本需要您进行两次认证。第一次是源账户，第二次是目标账户。")
    asyncio.run(main())
