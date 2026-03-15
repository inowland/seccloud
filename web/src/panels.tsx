import type { ReactNode } from "react";

export type SectionTone = "default" | "priority" | "detail";

export interface SectionProps {
  title?: string;
  titleHref?: string;
  subtitle?: string;
  tone?: SectionTone;
  children: ReactNode;
  footer?: ReactNode;
}

export interface EmptyStateProps {
  title: string;
  body: string;
}

export interface DetailSectionCardProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
  className?: string;
}

interface RecordPageProps {
  children: ReactNode;
}

export function Section({
  title,
  titleHref,
  subtitle,
  tone = "default",
  children,
  footer,
}: SectionProps) {
  return (
    <section className={`panel panel--${tone}`}>
      {title || subtitle ? (
        <div className="panel__header">
          <div>
            {title ? (
              <h2>
                {titleHref ? (
                  <a className="panel__header-link" href={titleHref}>
                    {title}
                  </a>
                ) : (
                  title
                )}
              </h2>
            ) : null}
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
        </div>
      ) : null}
      <div className="panel__body">{children}</div>
      {footer ? <div className="panel__footer">{footer}</div> : null}
    </section>
  );
}

export function EmptyState({ title, body }: EmptyStateProps) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{body}</p>
    </div>
  );
}

export function DetailSectionCard({
  title,
  subtitle,
  children,
  className,
}: DetailSectionCardProps) {
  return (
    <div
      className={
        className ? `detail-section-card ${className}` : "detail-section-card"
      }
    >
      <div className="detail-section-card__header">
        <strong>{title}</strong>
        {subtitle ? <p>{subtitle}</p> : null}
      </div>
      {children}
    </div>
  );
}

export function RecordPage({ children }: RecordPageProps) {
  return (
    <div className="record-page">
      <Section title="Details">{children}</Section>
    </div>
  );
}
