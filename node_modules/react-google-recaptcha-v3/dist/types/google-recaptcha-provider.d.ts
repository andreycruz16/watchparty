import React from 'react';
import { ReactNode } from 'react';
interface IGoogleReCaptchaProviderProps {
    reCaptchaKey?: string;
    language?: string;
    useRecaptchaNet?: boolean;
    useEnterprise?: boolean;
    scriptProps?: {
        nonce?: string;
        defer?: boolean;
        async?: boolean;
        appendTo?: 'head' | 'body';
        id?: string;
    };
    children: ReactNode;
}
export interface IGoogleReCaptchaConsumerProps {
    executeRecaptcha?: (action?: string) => Promise<string>;
}
declare const GoogleReCaptchaContext: React.Context<IGoogleReCaptchaConsumerProps>;
declare const GoogleReCaptchaConsumer: React.Consumer<IGoogleReCaptchaConsumerProps>;
export declare function GoogleReCaptchaProvider({ reCaptchaKey, useEnterprise, useRecaptchaNet, scriptProps, language, children }: IGoogleReCaptchaProviderProps): JSX.Element;
export { GoogleReCaptchaConsumer, GoogleReCaptchaContext };
//# sourceMappingURL=google-recaptcha-provider.d.ts.map