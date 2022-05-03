export interface IGoogleRecaptchaProps {
    onVerify: (token: string) => void | Promise<void>;
    action?: string;
}
export declare function GoogleReCaptcha({ action, onVerify }: IGoogleRecaptchaProps): null;
//# sourceMappingURL=google-recaptcha.d.ts.map