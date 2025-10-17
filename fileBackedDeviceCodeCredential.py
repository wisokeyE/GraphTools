from datetime import datetime
from typing import Optional, Callable, Any
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions, AuthenticationRecord
from azure.core.credentials import AccessToken
from azure.identity._constants import DEVELOPER_SIGN_ON_CLIENT_ID


class FileBackedDeviceCodeCredential(DeviceCodeCredential):
    """
    在原有的DeviceCodeCredential基础上增加一个令牌缓存为文件的功能
    """
    def __init__(
        self,
        client_id: str = DEVELOPER_SIGN_ON_CLIENT_ID,
        *,
        timeout: Optional[int] = None,
        prompt_callback: Optional[Callable[[str, str, datetime], None]] = None,
        file_path: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        self.file_path = file_path
        self.record_json = None
        if self.file_path:
            deserialized_record = kwargs.pop("authentication_record", None)
            if deserialized_record is None:
                try:
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        record_json = f.read()
                except (FileNotFoundError, IOError):
                    record_json = None
                deserialized_record = AuthenticationRecord.deserialize(record_json) if record_json else None
            cache_persistence_options = kwargs.pop("cache_persistence_options", TokenCachePersistenceOptions())
            super(FileBackedDeviceCodeCredential, self).__init__(
                client_id=client_id,
                timeout=timeout,
                prompt_callback=prompt_callback,
                cache_persistence_options=cache_persistence_options,
                authentication_record=deserialized_record,
                **kwargs
                )
        else:
            super(FileBackedDeviceCodeCredential, self).__init__(
                client_id=client_id,
                timeout=timeout,
                prompt_callback=prompt_callback,
                **kwargs
                )

    def save_record(self):
        if self.file_path and self._auth_record:
            record_json = self._auth_record.serialize()
            if self.record_json != record_json:
                self.record_json = record_json
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    f.write(self.record_json)

    def get_token(
        self,
        *scopes: str,
        claims: Optional[str] = None,
        tenant_id: Optional[str] = None,
        enable_cae: bool = False,
        **kwargs: Any,
    ) -> AccessToken:
        token = super(FileBackedDeviceCodeCredential, self).get_token(*scopes, claims=claims, tenant_id=tenant_id, enable_cae=enable_cae, **kwargs)
        self.save_record()
        return token
        